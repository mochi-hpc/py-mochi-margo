/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <string>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <mercury_proc_string.h>
#include <margo.h>
#include "base64.hpp"

namespace py11 = pybind11;
namespace np = py11;

typedef py11::capsule pymargo_instance_id;
typedef py11::capsule pymargo_addr;
typedef py11::capsule pymargo_bulk;

#define MID2CAPSULE(__mid)   py11::capsule((void*)(__mid), "margo_instance_id", nullptr)
#define ADDR2CAPSULE(__addr) py11::capsule((void*)(__addr), "hg_addr_t", nullptr)
#define BULK2CAPSULE(__blk)  py11::capsule((void*)(__blk), "hg_bulk_t", nullptr)
#define CAPSULE2MID(__caps)  (margo_instance_id)(__caps)
#define CAPSULE2ADDR(__caps) (hg_addr_t)(__caps)
#define CAPSULE2BULK(__caps) (hg_bulk_t)(__caps)

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
    pymargo_addr _get_hg_addr() const;
    std::string forward(uint16_t provider_id, const std::string& input);
    void respond(const std::string& output);
    pymargo_instance_id _get_mid() const;
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

enum pymargo_bulk_access_mode {
    PYMARGO_READ_ONLY  = HG_BULK_READ_ONLY,
    PYMARGO_WRITE_ONLY = HG_BULK_WRITE_ONLY,
    PYMARGO_READWRITE  = HG_BULK_READWRITE
};

enum pymargo_bulk_xfer_mode {
    PYMARGO_PUSH = HG_BULK_PUSH,
    PYMARGO_PULL = HG_BULK_PULL
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
    if(mid == MARGO_INSTANCE_NULL) {
        throw std::runtime_error("margo_init() returned MARGO_INSTANCE_NULL");
    }
    return MID2CAPSULE(mid);
}

static void pymargo_generic_finalize_cb(void* arg)
{
    //try {
        PyGILState_STATE gstate;
        gstate = PyGILState_Ensure();
        PyObject* pyobj = static_cast<PyObject*>(arg);
        py11::handle fun(pyobj);
        fun();
        PyGILState_Release(gstate);
    //} catch(const py11::error_already_set&) {
    //    PyErr_Print();
    //    exit(-1);
    //}
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
        std::stringstream ss;
        ss << "margo_get_input() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
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
        result = HG_OTHER_ERROR;
    } catch(const std::exception& ex) {
        std::cerr << ex.what();
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
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_register_data() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
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
    if(rpc_id == 0) {
        throw std::runtime_error("margo_register_provider() return rpc id 0");
    }
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
        std::stringstream ss;
        ss << "margo_registered_name() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
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
        std::stringstream ss;
        ss << "margo_registered_name_mplex() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
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
        std::stringstream ss;
        ss << "margo_addr_lookup() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
    return ADDR2CAPSULE(addr);
}

static void pymargo_addr_free(
        pymargo_instance_id mid,
        pymargo_addr pyaddr)
{
    hg_return_t ret;
    ret = margo_addr_free(mid, pyaddr);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_addr_free() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
}

static pymargo_addr pymargo_addr_self(
        pymargo_instance_id mid)
{
    hg_addr_t addr;
    hg_return_t ret;
    ret = margo_addr_self(mid, &addr);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_addr_self() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
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
        std::stringstream ss;
        ss << "margo_addr_dup() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
    return ADDR2CAPSULE(newaddr);
}

static std::string pymargo_addr_to_string(
        pymargo_instance_id mid,
        pymargo_addr addr)
{
    hg_size_t buf_size = 0;
    hg_return_t ret = margo_addr_to_string(mid, NULL, &buf_size, addr);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_addr_to_string() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
    std::string result(buf_size,' ');
    ret = margo_addr_to_string(mid, const_cast<char*>(result.data()), &buf_size, addr);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_addr_to_string() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
    result.resize(buf_size-1);
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
        std::stringstream ss;
        ss << "margo_create() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
    pymargo_hg_handle pyhandle(handle);
    return pyhandle;
}

pymargo_hg_handle::pymargo_hg_handle(const pymargo_hg_handle& other)
: handle(other.handle)
{
    hg_return_t ret = margo_ref_incr(handle);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_ref_incr() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
}

pymargo_hg_handle::~pymargo_hg_handle()
{
    margo_destroy(handle);
}

std::string pymargo_hg_handle::forward(uint16_t provider_id, const std::string& input)
{
    hg_string_t instr = const_cast<char*>(input.data());
    hg_return_t ret;
    Py_BEGIN_ALLOW_THREADS
    ret = margo_provider_forward(provider_id, handle, &instr);
    Py_END_ALLOW_THREADS

    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_provider_forward() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }

    hg_string_t out;
    ret = margo_get_output(handle, &out);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_get_output() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
    std::string result(out);
    ret = margo_free_output(handle, &out);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_free_output() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
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
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_respond() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
}

hg_id_t pymargo_hg_handle::get_id() const
{
    auto info = margo_get_info(handle);
    return info->id;
}

pymargo_addr pymargo_hg_handle::_get_hg_addr() const
{
    auto info = margo_get_info(handle);
    return ADDR2CAPSULE(info->addr);
}

pymargo_instance_id pymargo_hg_handle::_get_mid() const
{
    return MID2CAPSULE(margo_hg_handle_get_instance(handle));
}

pymargo_bulk pymargo_bulk_create(
        pymargo_instance_id mid,
        const np::array& data,
        pymargo_bulk_access_mode flags)
{
    if(!(data.flags() & (np::array::f_style | np::array::c_style))) {
        throw std::runtime_error("Non-contiguous numpy arrays not yet supported by PyMargo");
    }
    hg_size_t size = data.dtype().itemsize();
    for(int i = 0; i < data.ndim(); i++) {
        size *= data.shape(i);
    }
    void* buffer = const_cast<void*>(data.data());
    hg_bulk_t handle;
    hg_return_t ret = margo_bulk_create(mid, 1, &buffer, &size, flags, &handle);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_bulk_create() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
    return BULK2CAPSULE(handle);
}

void pymargo_bulk_free(pymargo_bulk bulk) {
    hg_return_t ret = margo_bulk_free(bulk);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_bulk_free() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
}

void pymargo_bulk_ref_incr(pymargo_bulk bulk) {
    hg_return_t ret = margo_bulk_ref_incr(bulk);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_bulk_ref_incr() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
}

void pymargo_bulk_transfer(
        pymargo_instance_id mid,
        pymargo_bulk_xfer_mode op,
        pymargo_addr origin_addr,
        pymargo_bulk origin_handle,
        size_t origin_offset,
        pymargo_bulk local_handle,
        size_t local_offset,
        size_t size) {
    
    hg_return_t ret = margo_bulk_transfer(mid, static_cast<hg_bulk_op_t>(op), origin_addr,
            origin_handle, origin_offset, local_handle, local_offset, size);
    
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_bulk_transfer() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
}

std::string pymargo_bulk_to_base64(
        pymargo_bulk handle, bool request_eager) {
    hg_bool_t flag = request_eager ? HG_TRUE : HG_FALSE;
    hg_size_t buf_size = margo_bulk_get_serialize_size(handle, flag);
    std::string raw_buf(buf_size, '\0');
    hg_return_t ret = margo_bulk_serialize(const_cast<char*>(raw_buf.data()), buf_size, flag, handle);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_bulk_serialize() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
    return pymargo_base64_encode(reinterpret_cast<unsigned char const*>(raw_buf.data()), buf_size);
}

py11::bytes pymargo_bulk_to_str(
        pymargo_bulk handle, bool request_eager) {

    hg_bool_t flag = request_eager ? HG_TRUE : HG_FALSE;
    hg_size_t buf_size = margo_bulk_get_serialize_size(handle, flag);
    std::string raw_buf(buf_size, '\0');
    hg_return_t ret = margo_bulk_serialize(const_cast<char*>(raw_buf.data()), buf_size, flag, handle);
    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_bulk_serialize() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }
    return raw_buf;
}

pymargo_bulk pymargo_base64_to_bulk(
        pymargo_instance_id mid,
        const std::string& rep) {

    std::string raw = pymargo_base64_decode(rep);

    hg_bulk_t handle;
    hg_return_t ret = margo_bulk_deserialize(mid, &handle, raw.data(), raw.size());

    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_bulk_deserialize() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }

    return BULK2CAPSULE(handle);
}

pymargo_bulk pymargo_str_to_bulk(
        pymargo_instance_id mid,
        const py11::bytes& rep) {

    hg_bulk_t handle;
    std::string str_rep = static_cast<std::string>(rep);
    hg_return_t ret = margo_bulk_deserialize(mid, &handle, str_rep.data(), str_rep.size());

    if(ret != HG_SUCCESS) {
        std::stringstream ss;
        ss << "margo_bulk_deserialize() failed (ret = " << ret << ")";
        throw std::runtime_error(ss.str());
    }

    return BULK2CAPSULE(handle);
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
    try { py11::module::import("numpy"); }
    catch (...) {
        std::cerr << "[PyMargo] Error: could not import numpy at C++ level" << std::endl;
        exit(-1);
    }

    py11::enum_<pymargo_mode>(m,"mode")
        .value("client", PYMARGO_CLIENT_MODE)
        .value("server", PYMARGO_SERVER_MODE)
        ;
    py11::enum_<pymargo_rpc_mode>(m,"location")
        .value("in_caller_thread", PYMARGO_IN_CALLER_THREAD)
        .value("in_progress_thread", PYMARGO_IN_PROGRESS_THREAD)
        ;
    py11::enum_<pymargo_bulk_access_mode>(m, "access")
        .value("read_only",  PYMARGO_READ_ONLY)
        .value("write_only", PYMARGO_WRITE_ONLY)
        .value("read_write", PYMARGO_READWRITE)
        ;
    py11::enum_<pymargo_bulk_xfer_mode>(m, "xfer")
        .value("push", PYMARGO_PUSH)
        .value("pull", PYMARGO_PULL)
        ;
    py11::class_<pymargo_hg_handle>(m,"Handle")
        .def("_get_hg_addr", &pymargo_hg_handle::_get_hg_addr)
        .def("get_id", &pymargo_hg_handle::get_id)
        .def("forward", &pymargo_hg_handle::forward)
        .def("respond", &pymargo_hg_handle::respond)
        .def("_get_mid", &pymargo_hg_handle::_get_mid)
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

    m.def("bulk_create",              &pymargo_bulk_create);
    m.def("bulk_free",                &pymargo_bulk_free);
    m.def("bulk_ref_incr",            &pymargo_bulk_ref_incr);
    m.def("bulk_transfer",            &pymargo_bulk_transfer);
    m.def("bulk_to_base64",           &pymargo_bulk_to_base64);
    m.def("base64_to_bulk",           &pymargo_base64_to_bulk);
    m.def("bulk_to_str",              &pymargo_bulk_to_str);
    m.def("str_to_bulk",              &pymargo_str_to_bulk);

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
