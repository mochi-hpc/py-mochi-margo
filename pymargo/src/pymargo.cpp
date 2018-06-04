/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <string>
#include <iostream>
#include <mercury_proc_string.h>
#include <margo.h>

namespace py11 = pybind11;

typedef py11::capsule pymargo_instance_id;
typedef py11::capsule pymargo_addr;

#define MID2CAPSULE(__mid)   py11::capsule((void*)(__mid), "margo_instance_id", nullptr)
#define ADDR2CAPSULE(__addr) py11::capsule((void*)(__addr), "hg_addr_t", nullptr)
#define CAPSULE2MID(__caps)  (margo_instance_id)(__caps)
#define CAPSULE2ADDR(__caps) (hg_addr_t)(__caps)

struct __attribute__ ((visibility("hidden"))) pymargo_rpc_data {
    py11::object obj;
    std::string method;
};

struct pymargo_hg_handle {
    hg_handle_t handle;
    pymargo_hg_handle(hg_handle_t h)
        : handle(h) {}
    pymargo_hg_handle(const pymargo_hg_handle& other);
    ~pymargo_hg_handle();
    hg_id_t get_id() const;
    pymargo_addr get_addr() const;
    std::string forward(uint16_t provider_id, const std::string& input);
    void respond(const std::string& output);
    pymargo_instance_id get_mid() const;
};

void delete_rpc_data(void* arg) {
    pymargo_rpc_data* rpc_data = static_cast<pymargo_rpc_data*>(arg);
    delete rpc_data;
}

enum pymargo_mode {
    PYMARGO_CLIENT_MODE = MARGO_CLIENT_MODE,
    PYMARGO_SERVER_MODE = MARGO_SERVER_MODE
};

enum pymargo_rpc_mode {
    PYMARGO_IN_CALLER_THREAD,
    PYMARGO_IN_PROGRESS_THREAD
};

static pymargo_instance_id pymargo_init(
        const std::string& addr,
        pymargo_mode mode,
        bool use_progress_thread,
        pymargo_rpc_mode loc)
{
    int l = loc == PYMARGO_IN_CALLER_THREAD ? 0 : -1;
    if(mode == PYMARGO_CLIENT_MODE) l = 0;
    margo_instance_id mid = margo_init(addr.c_str(), mode, (int)use_progress_thread, l);
    return MID2CAPSULE(mid);
}

static void pymargo_generic_finalize_cb(void* arg)
{
    try {
        PyGILState_STATE gstate;
        gstate = PyGILState_Ensure();
        PyObject* pyobj = static_cast<PyObject*>(arg);
        py11::handle fun(pyobj); 
        fun();
        PyGILState_Release(gstate);
    } catch(const py11::error_already_set&) {
        PyErr_Print();
        exit(-1);
    }
}

static void pymargo_push_finalize_callback(
        pymargo_instance_id mid,
        py11::object cb)
{
    Py_INCREF(cb.ptr());
    margo_push_finalize_callback(
            mid, 
            &pymargo_generic_finalize_cb,
            static_cast<void*>(cb.ptr()));
}

static hg_return_t pymargo_generic_rpc_callback(hg_handle_t handle)
{
    hg_return_t result         = HG_SUCCESS;
    hg_return_t ret            = HG_SUCCESS;
    margo_instance_id mid      = MARGO_INSTANCE_NULL;
    const struct hg_info* info = NULL;
    hg_string_t input;

    mid  = margo_hg_handle_get_instance(handle);
    info = margo_get_info(handle);
    ret  = margo_get_input(handle, &input);

    if(ret != HG_SUCCESS) {
        result = ret;
        margo_free_input(handle, &input);
        margo_destroy(handle);
        return result;
    }

    pymargo_rpc_data* rpc_data = NULL;
    void* data = margo_registered_data(mid, info->id);
    rpc_data = static_cast<pymargo_rpc_data*>(data);

    if(!rpc_data) {
        std::cerr << "[Py-Margo] ERROR: Received a request for a provider id "
            << " with no registered provider" << std::endl;
        return HG_OTHER_ERROR;
    }

    std::string out;
    try {
        PyGILState_STATE gstate;
        gstate = PyGILState_Ensure();
        pymargo_hg_handle pyhandle(handle);
        margo_ref_incr(handle);
        py11::object fun = rpc_data->obj.attr(rpc_data->method.c_str());
        py11::object r = fun(pyhandle, std::string(input));
        PyGILState_Release(gstate);
    } catch(const py11::error_already_set&) {
        PyErr_Print();
        exit(-1);
    }

    ret = margo_free_input(handle, &input);
    if(ret != HG_SUCCESS) result = ret;

    ret = margo_destroy(handle);
    if(ret != HG_SUCCESS) result = ret;

    return result;
}
DEFINE_MARGO_RPC_HANDLER(pymargo_generic_rpc_callback)

static hg_id_t pymargo_register(
        pymargo_instance_id mid,
        const std::string& rpc_name,
        uint16_t provider_id,
        py11::object obj,
        const std::string& method_name)
{
    int ret;

    hg_id_t rpc_id;
    rpc_id = MARGO_REGISTER_PROVIDER(mid, rpc_name.c_str(),
            hg_string_t, hg_string_t, pymargo_generic_rpc_callback,
            provider_id, ABT_POOL_NULL);

    pymargo_rpc_data* rpc_data = new pymargo_rpc_data;
    rpc_data->obj    = obj;
    rpc_data->method = method_name;

    ret = margo_register_data(mid, rpc_id,
                static_cast<void*>(rpc_data), delete_rpc_data);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_register_data() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    return rpc_id;
}

static hg_id_t pymargo_register_on_client(
        pymargo_instance_id mid,
        const std::string& rpc_name,
        uint16_t provider_id)
{
    hg_id_t rpc_id;

    rpc_id = MARGO_REGISTER_PROVIDER(
                    mid,
                    rpc_name.c_str(),
                    hg_string_t, hg_string_t, NULL,
                    provider_id, ABT_POOL_NULL);
    // TODO throw an exception if the return value is not correct
    return rpc_id;
}

static py11::object pymargo_registered(
        pymargo_instance_id mid,
        const std::string& rpc_name)
{
    hg_id_t id;
    hg_bool_t flag;
    hg_return_t ret;

    ret = margo_registered_name(
            mid,
            rpc_name.c_str(), &id, &flag);

    if(ret != HG_SUCCESS) {
        std::cerr << "margo_registered_name() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
    if(flag) {
        return py11::cast(id);
    } else {
        return py11::none();
    }
}

static py11::object pymargo_provider_registered(
        pymargo_instance_id mid,
        const std::string& rpc_name,
        uint16_t provider_id)
{
    hg_id_t id;
    hg_bool_t flag;
    hg_return_t ret;

    ret = margo_provider_registered_name(
            mid, 
            rpc_name.c_str(), provider_id, &id, &flag);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_registered_name_mplex() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
    if(flag) {
        return py11::cast(id);
    } else {
        return py11::none();
    }
}

static pymargo_addr pymargo_lookup(
        pymargo_instance_id mid,
        const std::string& addrstr)
{
    hg_addr_t addr;
    hg_return_t ret;

    ret = margo_addr_lookup(
            mid, 
            addrstr.c_str(), &addr);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_addr_lookup() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
    return ADDR2CAPSULE(addr);
}

static void pymargo_addr_free(
        pymargo_instance_id mid,
        pymargo_addr pyaddr)
{
    hg_return_t ret;
    ret = margo_addr_free(mid, pyaddr);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_addr_free() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
}

static pymargo_addr pymargo_addr_self(
        pymargo_instance_id mid)
{
    hg_addr_t addr;
    hg_return_t ret;
    ret = margo_addr_self(mid, &addr);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_addr_self() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
    return ADDR2CAPSULE(addr);
}

static pymargo_addr pymargo_addr_dup(
        pymargo_instance_id mid,
        pymargo_addr addr)
{
    hg_addr_t newaddr;
    hg_return_t ret;
    ret = margo_addr_dup(mid, addr, &newaddr);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_addr_dup() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    return ADDR2CAPSULE(newaddr);
}

static std::string pymargo_addr_to_string(
        pymargo_instance_id mid,
        pymargo_addr addr)
{
    hg_size_t buf_size = 0;
    margo_addr_to_string(mid, NULL, &buf_size, addr);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    std::string result(buf_size,' ');
    margo_addr_to_string(mid, const_cast<char*>(result.data()), &buf_size, addr);
    result.resize(buf_size-1);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    return result;
}

static pymargo_hg_handle pymargo_create(
        pymargo_instance_id mid,
        pymargo_addr addr,
        hg_id_t id)
{
    hg_handle_t handle;
    hg_return_t ret;
    ret = margo_create(mid, addr, id, &handle);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_create() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
    pymargo_hg_handle pyhandle(handle);
    return pyhandle;
}

pymargo_hg_handle::pymargo_hg_handle(const pymargo_hg_handle& other)
: handle(other.handle)
{
    margo_ref_incr(handle);
    // TODO throw an exception if the return value is not  HG_SUCCESS
}

pymargo_hg_handle::~pymargo_hg_handle()
{
    margo_destroy(handle);
}

std::string pymargo_hg_handle::forward(uint16_t provider_id, const std::string& input)
{
    hg_string_t instr = const_cast<char*>(input.data());
    Py_BEGIN_ALLOW_THREADS
    margo_provider_forward(provider_id, handle, &instr);
    Py_END_ALLOW_THREADS

    // TODO throw an exception if the return value is not  HG_SUCCESS
    hg_string_t out;
    margo_get_output(handle, &out);
    std::string result(out);
    margo_free_output(handle, &out);
    return result;
}
/*
static void pymargo_forward_timed(
        pymargo_hg_handle pyhandle,
        const std::string& input,
        double timeout_ms)
{
    margo_forward_timed(pyhandle.handle, const_cast<char*>(input.data()), timeout_ms);
    // TODO throw an exception if the return value is not  HG_SUCCESS
}
*/
void pymargo_hg_handle::respond(const std::string& output)
{
    hg_string_t out;
    out = const_cast<char*>(output.data());
    hg_return_t ret;
    Py_BEGIN_ALLOW_THREADS
    ret = margo_respond(handle, &out);
    Py_END_ALLOW_THREADS
    // TODO throw an exception if the return value is not  HG_SUCCESS
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_respond() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
}

hg_id_t pymargo_hg_handle::get_id() const
{
    auto info = margo_get_info(handle);
    return info->id;
}

pymargo_addr pymargo_hg_handle::get_addr() const
{
    auto info = margo_get_info(handle);
    return ADDR2CAPSULE(info->addr);
}

pymargo_instance_id pymargo_hg_handle::get_mid() const
{
    return MID2CAPSULE(margo_hg_handle_get_instance(handle));
}

///////////////////////////////////////////////////////////////////////////////////
// ABT namespace
///////////////////////////////////////////////////////////////////////////////////
class py_abt_mutex {
        
    ABT_mutex mutex_m;

    public:

    explicit py_abt_mutex(bool recursive = false) {
        ABT_mutex_attr attr;
        ABT_mutex_attr_create(&attr);
        if(recursive)
            ABT_mutex_attr_set_recursive(attr, ABT_TRUE);
        ABT_mutex_create_with_attr(attr, &mutex_m);
        ABT_mutex_attr_free(&attr);
    }

    py_abt_mutex(const py_abt_mutex& other) = delete;

    py_abt_mutex(py_abt_mutex&& other) {
        mutex_m = other.mutex_m;
        other.mutex_m = ABT_MUTEX_NULL;
    }

    ~py_abt_mutex() noexcept {
        if(mutex_m != ABT_MUTEX_NULL)
            ABT_mutex_free(&mutex_m);
    }

    int lock() noexcept {
        int ret;
        Py_BEGIN_ALLOW_THREADS
        ret = ABT_mutex_lock(mutex_m);
        Py_END_ALLOW_THREADS
        return ret;
    }

    int unlock() noexcept {
        int ret;
        Py_BEGIN_ALLOW_THREADS
        ret = ABT_mutex_unlock(mutex_m);
        Py_END_ALLOW_THREADS
        return ret;
    }
};

class py_abt_rwlock {
        
    ABT_rwlock lock_m;

    public:

    explicit py_abt_rwlock() {
        ABT_rwlock_create(&lock_m);
    }

    py_abt_rwlock(const py_abt_rwlock&) = delete;

    py_abt_rwlock(py_abt_rwlock&& other) {
        lock_m = other.lock_m;
        other.lock_m = ABT_RWLOCK_NULL;
    }

    ~py_abt_rwlock() noexcept {
        if(lock_m != ABT_RWLOCK_NULL)
            ABT_rwlock_free(&lock_m);
    }

    int rdlock() noexcept {
        int ret;
        Py_BEGIN_ALLOW_THREADS
        ret = ABT_rwlock_rdlock(lock_m);
        Py_END_ALLOW_THREADS
        return ret;
    }

    int wrlock() noexcept {
        int ret;
        Py_BEGIN_ALLOW_THREADS
        ret = ABT_rwlock_wrlock(lock_m);
        Py_END_ALLOW_THREADS
        return ret;
    }

    int unlock() noexcept {
        int ret;
        Py_BEGIN_ALLOW_THREADS
        ret = ABT_rwlock_unlock(lock_m);
        Py_END_ALLOW_THREADS
        return ret;
    }
};

static int py_abt_yield() {
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = ABT_thread_yield();
    Py_END_ALLOW_THREADS
    return ret;
}

///////////////////////////////////////////////////////////////////////////////////
PYBIND11_MODULE(_pymargo, m)
{
    py11::enum_<pymargo_mode>(m,"mode")
        .value("client", PYMARGO_CLIENT_MODE)
        .value("server", PYMARGO_SERVER_MODE)
        ;
    py11::enum_<pymargo_rpc_mode>(m,"location")
        .value("in_caller_thread", PYMARGO_IN_CALLER_THREAD)
        .value("in_progress_thread", PYMARGO_IN_PROGRESS_THREAD)
        ;
    py11::class_<pymargo_hg_handle>(m,"MargoHandle")
        .def("get_hg_addr", &pymargo_hg_handle::get_addr)
        .def("get_id", &pymargo_hg_handle::get_id)
        .def("forward", &pymargo_hg_handle::forward)
        .def("respond", &pymargo_hg_handle::respond)
        .def("get_mid", &pymargo_hg_handle::get_mid)
        ;

    m.def("init",                     &pymargo_init);
    m.def("finalize", [](pymargo_instance_id mid) {
            margo_finalize(mid);
    });
    m.def("wait_for_finalize",  [](pymargo_instance_id mid) {
            margo_wait_for_finalize(mid);
    });

    m.def("push_finalize_callback",   &pymargo_push_finalize_callback);
    m.def("enable_remote_shutdown", [](pymargo_instance_id mid) {
            margo_enable_remote_shutdown(mid);
    });
    m.def("shutdown_remote_instance", [](pymargo_instance_id mid, pymargo_addr addr) {
            margo_shutdown_remote_instance(mid, addr);
    });
    m.def("register",                 &pymargo_register);
    m.def("register_on_client",       &pymargo_register_on_client);
    m.def("registered",               &pymargo_registered);
    m.def("registered_mplex",         &pymargo_provider_registered);
    m.def("lookup",                   &pymargo_lookup);
    m.def("addr_free",                &pymargo_addr_free);
    m.def("addr_self",                &pymargo_addr_self);
    m.def("addr_dup",                 &pymargo_addr_dup);
    m.def("addr2str",                 &pymargo_addr_to_string);
    m.def("create",                   &pymargo_create);

    // Inside abt package
    py11::module abt_package = m.def_submodule("abt");
    abt_package.def("yield", &py_abt_yield);
    py11::class_<py_abt_mutex>(abt_package,"Mutex")
        .def(py11::init<bool>())
        .def("lock", &py_abt_mutex::lock)
        .def("unlock", &py_abt_mutex::unlock);
    py11::class_<py_abt_rwlock>(abt_package, "RWLock")
        .def("rdlock", &py_abt_rwlock::rdlock)
        .def("wrlock", &py_abt_rwlock::wrlock)
        .def("unlock", &py_abt_rwlock::unlock);

#undef ret_policy_opaque
}
