/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <string>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <mercury_proc_string.h>
#include <margo.h>

namespace py11 = pybind11;
namespace np = py11;

using namespace pybind11::literals;

typedef py11::capsule pymargo_instance_id;
typedef py11::capsule pymargo_addr;
typedef py11::capsule pymargo_bulk;
typedef py11::capsule pymargo_request;

#define MID2CAPSULE(__mid)   py11::capsule((void*)(__mid), "margo_instance_id", nullptr)
#define ADDR2CAPSULE(__addr) py11::capsule((void*)(__addr), "hg_addr_t", nullptr)
#define BULK2CAPSULE(__blk)  py11::capsule((void*)(__blk), "hg_bulk_t", nullptr)
#define REQ2CAPSULE(__req)   py11::capsule((void*)(__req), "margo_request", nullptr)
#define CAPSULE2MID(__caps)  (margo_instance_id)(__caps)
#define CAPSULE2ADDR(__caps) (hg_addr_t)(__caps)
#define CAPSULE2BULK(__caps) (hg_bulk_t)(__caps)
#define CAPSULE2REQ(__caps)  (margo_request)(__caps)

struct __attribute__ ((visibility("hidden"))) pymargo_rpc_data {
    py11::object callable;
};

struct pymargo_hg_handle {
    hg_handle_t handle;
    pymargo_hg_handle(hg_handle_t h)
        : handle(h) {}
    pymargo_hg_handle(const pymargo_hg_handle& other);
    ~pymargo_hg_handle();
    hg_id_t get_id() const;
    pymargo_addr _get_hg_addr() const;
    py11::object forward(uint16_t provider_id, py11::bytes input, double timeout);
    pymargo_request iforward(uint16_t provider_id, py11::bytes input, double timeout);
    void respond(py11::bytes output);
    pymargo_request irespond(py11::bytes output);
    pymargo_instance_id _get_mid() const;
    py11::object _get_output() const;
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

struct pymargo_exception : public std::runtime_error {

    pymargo_exception(
        const std::string& fun_name,
        hg_return_t ret)
    : std::runtime_error(fun_name + "() returned " + std::to_string(ret))
    , code(ret) {}

    hg_return_t code;
};

static hg_return_t hg_proc_py11bytes(hg_proc_t proc, void *data) {
    py11::bytes* bytes = static_cast<py11::bytes*>(data);
    hg_return ret = HG_SUCCESS;
    switch(hg_proc_get_op(proc)) {
    case HG_DECODE: {
        ssize_t len = 0;
        ret = hg_proc_hg_size_t(proc, &len);
        if(ret != HG_SUCCESS) return ret;
        void* buf = hg_proc_save_ptr(proc, len);
        *bytes = py11::bytes((char*)buf, len);
        hg_proc_restore_ptr(proc, buf, len);
        break;
    }
    case HG_ENCODE: {
        char* data = nullptr;
        ssize_t len = 0;
        if(PyBytes_AsStringAndSize(bytes->ptr(), &data, &len)) {
            throw py11::error_already_set();
        }
        ret = hg_proc_hg_size_t(proc, &len);
        if(ret != HG_SUCCESS) return ret;
        void* buf = hg_proc_save_ptr(proc, len);
        memcpy(buf, data, len);
        hg_proc_restore_ptr(proc, buf, len);
        break;
    }
    case HG_FREE:
        break;
    }
    return ret;
}

static pymargo_instance_id pymargo_init(
        const std::string& addr,
        pymargo_mode mode,
        bool use_progress_thread,
        int num_rpc_threads,
        const std::string& config)
{
    int l = num_rpc_threads;
    if(mode == PYMARGO_CLIENT_MODE) l = 0;
    margo_instance_id mid;
    if(config.empty()) {
        mid = margo_init(addr.c_str(), mode, (int)use_progress_thread, l);
    } else {
        struct margo_init_info info;
        std::memset(&info, 0, sizeof(info));
        info.json_config = config.c_str();
        mid = margo_init_ext(addr.c_str(), mode, &info);
    }
    if(mid == MARGO_INSTANCE_NULL) {
        throw std::runtime_error("margo_init() returned MARGO_INSTANCE_NULL");
    }
    return MID2CAPSULE(mid);
}

static void pymargo_generic_finalize_cb(void* arg)
{
    py11::gil_scoped_acquire acquire;
    PyObject* pyobj = static_cast<PyObject*>(arg);
    py11::handle fun(pyobj);
    fun();
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

static void pymargo_push_prefinalize_callback(
        pymargo_instance_id mid,
        py11::object cb)
{
    Py_INCREF(cb.ptr());
    margo_push_prefinalize_callback(
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
    py11::bytes input;

    mid  = margo_hg_handle_get_instance(handle);
    info = margo_get_info(handle);
    ret  = margo_get_input(handle, &input);

    if(ret != HG_SUCCESS) {
        result = ret;
        margo_free_input(handle, &input);
        margo_destroy(handle);
        throw pymargo_exception("margo_get_input", ret);
    }

    pymargo_rpc_data* rpc_data = NULL;
    void* data = margo_registered_data(mid, info->id);
    rpc_data = static_cast<pymargo_rpc_data*>(data);

    if(!rpc_data) {
        std::cerr << "[Py-Margo] ERROR: Received a request for a provider id "
            << " with no registered provider" << std::endl;
        return HG_OTHER_ERROR;
    }

    int disabled_flag;
    margo_registered_disabled_response(mid, info->id, &disabled_flag);

    if(!disabled_flag) {
        std::string out;
        py11::gil_scoped_acquire acquire;
        try {
            pymargo_hg_handle pyhandle(handle);
            margo_ref_incr(handle);
            py11::object r = rpc_data->callable(pyhandle, input);
        } catch(const py11::error_already_set& e) {
            std::cerr << "[Py-Margo] ERROR: " << e.what() << std::endl;
            result = HG_OTHER_ERROR;
        } catch(const std::exception& ex) {
            std::cerr << ex.what();
            std::exit(-1);
        }
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
        py11::object callable)
{
    hg_return_t ret;

    hg_id_t rpc_id;
    rpc_id = MARGO_REGISTER_PROVIDER(mid, rpc_name.c_str(),
            py11bytes, py11bytes, pymargo_generic_rpc_callback,
            provider_id, ABT_POOL_NULL);

    pymargo_rpc_data* rpc_data = new pymargo_rpc_data;
    rpc_data->callable = callable;

    ret = margo_register_data(mid, rpc_id,
                static_cast<void*>(rpc_data), delete_rpc_data);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_register_data", ret);
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
                    py11bytes, py11bytes, NULL,
                    provider_id, ABT_POOL_NULL);
    if(rpc_id == 0) {
        throw pymargo_exception("margo_register_provider", (hg_return_t)0);
    }
    return rpc_id;
}

static void pymargo_deregister(
        pymargo_instance_id mid,
        hg_id_t id)
{
    hg_return_t ret;
    ret = margo_deregister(mid, id);

    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_deregister", ret);
    }
}

static void pymargo_disable_response(
        pymargo_instance_id mid,
        hg_id_t id, bool disable_flag)
{
    hg_return_t ret;
    ret =  margo_registered_disable_response(mid, id, disable_flag);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_deregistered_disable_response", ret);
    }
}

static bool pymargo_disabled_response(
        pymargo_instance_id mid,
        hg_id_t id)
{
    hg_return_t ret;
    int disable_flag;
    ret =  margo_registered_disabled_response(mid, id, &disable_flag);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_deregistered_disabled_response", ret);
    }
    return static_cast<bool>(disable_flag);
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
        throw pymargo_exception("margo_registered_name", ret);
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
        throw pymargo_exception("margo_provider_registered_name", ret);
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
        throw pymargo_exception("margo_addr_lookup", ret);
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
        throw pymargo_exception("margo_addr_free", ret);
    }
}

static pymargo_addr pymargo_addr_self(
        pymargo_instance_id mid)
{
    hg_addr_t addr;
    hg_return_t ret;
    ret = margo_addr_self(mid, &addr);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_addr_self", ret);
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
        throw pymargo_exception("margo_addr_dup", ret);
    }
    return ADDR2CAPSULE(newaddr);
}

static bool pymargo_addr_cmp(
        pymargo_instance_id mid,
        pymargo_addr addr1,
        pymargo_addr addr2)
{
    return margo_addr_cmp(mid, addr1, addr2);
}

static void pymargo_addr_set_remove(
        pymargo_instance_id mid,
        pymargo_addr addr)
{
    hg_return_t ret = margo_addr_set_remove(mid, addr);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_addr_set_remove", ret);
    }
}

static std::string pymargo_addr_to_string(
        pymargo_instance_id mid,
        pymargo_addr addr)
{
    hg_size_t buf_size = 0;
    hg_return_t ret = margo_addr_to_string(mid, NULL, &buf_size, addr);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_addr_to_string", ret);
    }
    std::string result(buf_size,' ');
    ret = margo_addr_to_string(mid, const_cast<char*>(result.data()), &buf_size, addr);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_addr_to_string", ret);
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
        throw pymargo_exception("margo_create", ret);
    }
    pymargo_hg_handle pyhandle(handle);
    return pyhandle;
}

pymargo_hg_handle::pymargo_hg_handle(const pymargo_hg_handle& other)
: handle(other.handle)
{
    hg_return_t ret = margo_ref_incr(handle);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_ref_incr", ret);
    }
}

pymargo_hg_handle::~pymargo_hg_handle()
{
    margo_destroy(handle);
}

py11::object pymargo_hg_handle::forward(uint16_t provider_id,
                                        py11::bytes input,
                                        double timeout)
{
    hg_return_t ret;

    Py_BEGIN_ALLOW_THREADS
    ret = margo_provider_forward_timed(provider_id, handle, &input, timeout);
    Py_END_ALLOW_THREADS
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_provider_forward_timed", ret);
    }
    return _get_output();
}

py11::object pymargo_hg_handle::_get_output() const {
    hg_return_t ret;
    int disabled_flag;
    auto mid = margo_hg_handle_get_instance(handle);
    auto info = margo_get_info(handle);

    margo_registered_disabled_response(mid, info->id, &disabled_flag);

    if(!disabled_flag) {
        py11::bytes out;
        ret = margo_get_output(handle, &out);
        if(ret == HG_TIMEOUT) {
            PyErr_SetString(PyExc_TimeoutError, "Margo forward timed out");
            throw py11::error_already_set();
        }
        if(ret != HG_SUCCESS) {
            throw pymargo_exception("margo_get_output", ret);
        }
        ret = margo_free_output(handle, &out);
        if(ret != HG_SUCCESS) {
            throw pymargo_exception("margo_free_output", ret);
        }
        return out;
    } else {
        return py11::none();
    }
}

pymargo_request pymargo_hg_handle::iforward(uint16_t provider_id,
                                            py11::bytes input,
                                            double timeout)
{
    hg_return_t ret;
    margo_request req;

    Py_BEGIN_ALLOW_THREADS
    ret = margo_provider_iforward_timed(provider_id, handle, &input, timeout, &req);
    Py_END_ALLOW_THREADS
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_provider_iforward_timed", ret);
    }
    return pymargo_request(req);
}

void pymargo_hg_handle::respond(py11::bytes output)
{
    hg_return_t ret;
    Py_BEGIN_ALLOW_THREADS
    ret = margo_respond(handle, &output);
    Py_END_ALLOW_THREADS
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_respond", ret);
    }
}

pymargo_request pymargo_hg_handle::irespond(py11::bytes output)
{
    hg_return_t ret;
    margo_request req;
    Py_BEGIN_ALLOW_THREADS
    ret = margo_irespond(handle, &output, &req);
    Py_END_ALLOW_THREADS
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_respond", ret);
    }
    return REQ2CAPSULE(req);
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

#define CHECK_BUFFER_IS_CONTIGUOUS(__buf_info__) do { \
    ssize_t __stride__ = (__buf_info__).itemsize;     \
    for(ssize_t i=0; i < (__buf_info__).ndim; i++) {  \
        if(__stride__ != (__buf_info__).strides[i])   \
            throw std::invalid_argument("Non-contiguous arrays not yet supported by PyMargo"); \
        __stride__ *= (__buf_info__).shape[i];        \
    }                                                 \
} while(0)

static py11::str pymargo_get_config(pymargo_instance_id mid) {
    char* config = margo_get_config(mid);
    if(!config) return py11::str();
    else {
        auto result = py11::str(config);
        free(config);
        return result;
    }
}

static pymargo_bulk pymargo_bulk_create(
        pymargo_instance_id mid,
        const py11::buffer& data,
        pymargo_bulk_access_mode flags)
{
    py11::buffer_info buf_info = data.request();
    CHECK_BUFFER_IS_CONTIGUOUS(buf_info);

    hg_size_t size = buf_info.itemsize * buf_info.size;
    void* buffer = const_cast<void*>(buf_info.ptr);
    hg_bulk_t handle;
    hg_return_t ret = margo_bulk_create(mid, 1, &buffer, &size, flags, &handle);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_bulk_create", ret);
    }
    return BULK2CAPSULE(handle);
}

static void pymargo_bulk_free(pymargo_bulk bulk) {
    hg_return_t ret = margo_bulk_free(bulk);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_bulk_free", ret);
    }
}

static void pymargo_bulk_ref_incr(pymargo_bulk bulk) {
    hg_return_t ret = margo_bulk_ref_incr(bulk);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_bulk_ref_incr", ret);
    }
}

static void pymargo_bulk_transfer(
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
        throw pymargo_exception("margo_bulk_transfer", ret);
    }
}

static pymargo_request pymargo_bulk_itransfer(
        pymargo_instance_id mid,
        pymargo_bulk_xfer_mode op,
        pymargo_addr origin_addr,
        pymargo_bulk origin_handle,
        size_t origin_offset,
        pymargo_bulk local_handle,
        size_t local_offset,
        size_t size) {

    margo_request req;
    hg_return_t ret = margo_bulk_itransfer(mid, static_cast<hg_bulk_op_t>(op), origin_addr,
            origin_handle, origin_offset, local_handle, local_offset, size, &req);

    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_bulk_itransfer", ret);
    }
    return REQ2CAPSULE(req);
}

static py11::bytes pymargo_bulk_to_str(
        pymargo_bulk handle, bool request_eager) {

    hg_bool_t flag = request_eager ? HG_TRUE : HG_FALSE;
    hg_size_t buf_size = margo_bulk_get_serialize_size(handle, flag);
    std::string raw_buf(buf_size, '\0');
    hg_return_t ret = margo_bulk_serialize(const_cast<char*>(raw_buf.data()), buf_size, flag, handle);
    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_bulk_serialize", ret);
    }
    return raw_buf;
}

static pymargo_bulk pymargo_str_to_bulk(
        pymargo_instance_id mid,
        const py11::bytes& rep) {

    hg_bulk_t handle;
    std::string str_rep = static_cast<std::string>(rep);
    hg_return_t ret = margo_bulk_deserialize(mid, &handle, str_rep.data(), str_rep.size());

    if(ret != HG_SUCCESS) {
        throw pymargo_exception("margo_bulk_deserialize", ret);
    }

    return BULK2CAPSULE(handle);
}

static void pymargo_thread_sleep(
        pymargo_instance_id mid,
        double t) {
    py11::gil_scoped_release release;
    margo_thread_sleep(mid, t);
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
// Logging
///////////////////////////////////////////////////////////////////////////////////

static int pymargo_set_global_logger(py11::object logger) {
    auto m = py11::module_::import("_pymargo");
    m.attr("__global_logger__") = logger;
#define LOG_FN(__level__)                           \
    [](void*, const char* msg) {                    \
        auto m = py11::module_::import("_pymargo"); \
        if(!hasattr(m, "__global_logger__")) return;\
        auto logger = m.attr("__global_logger__");  \
        logger.attr(#__level__)(msg);               \
    }

    margo_logger lgr = {
        nullptr,
        LOG_FN(trace),
        LOG_FN(debug),
        LOG_FN(info),
        LOG_FN(warning),
        LOG_FN(error),
        LOG_FN(critical)
    };
#undef LOG_FN

    return margo_set_global_logger(&lgr);
}

static int pymargo_set_logger(py11::object engine, py11::object logger) {
    engine.attr("_internal_logger") = logger;
#define LOG_FN(__level__)                              \
    [](void* e, const char* msg) {                     \
        auto engine = py11::handle((PyObject*)e);      \
        if(!hasattr(engine,"_internal_logger")) return;\
        auto logger = engine.attr("_internal_logger"); \
        logger.attr(#__level__)(msg);                  \
    }

    margo_logger lgr = {
        (void*)engine.ptr(),
        LOG_FN(trace),
        LOG_FN(debug),
        LOG_FN(info),
        LOG_FN(warning),
        LOG_FN(error),
        LOG_FN(critical)
    };
#undef LOG_FN
    pymargo_instance_id mid = engine.attr("_mid").cast<pymargo_instance_id>();

    return margo_set_logger(mid, &lgr);
}

static int pymargo_set_log_level(pymargo_instance_id mid, margo_log_level level) {
    return margo_set_log_level(mid, level);
}

static void pymargo_trace(const char* msg) {
    margo_trace(MARGO_INSTANCE_NULL, msg);
}

static void pymargo_trace(const char* msg, pymargo_instance_id mid) {
    margo_trace(mid, msg);
}

static void pymargo_debug(const char* msg) {
    margo_debug(MARGO_INSTANCE_NULL, msg);
}

static void pymargo_debug(const char* msg, pymargo_instance_id mid) {
    margo_debug(mid, msg);
}

static void pymargo_info(const char* msg) {
    margo_info(MARGO_INSTANCE_NULL, msg);
}

static void pymargo_info(const char* msg, pymargo_instance_id mid) {
    margo_info(mid, msg);
}

static void pymargo_warning(const char* msg) {
    margo_warning(MARGO_INSTANCE_NULL, msg);
}

static void pymargo_warning(const char* msg, pymargo_instance_id mid) {
    margo_warning(mid, msg);
}

static void pymargo_error(const char* msg) {
    margo_error(MARGO_INSTANCE_NULL, msg);
}

static void pymargo_error(const char* msg, pymargo_instance_id mid) {
    margo_error(mid, msg);
}

static void pymargo_critical(const char* msg) {
    margo_critical(MARGO_INSTANCE_NULL, msg);
}

static void pymargo_critical(const char* msg, pymargo_instance_id mid) {
    margo_critical(mid, msg);
}

///////////////////////////////////////////////////////////////////////////////////
PYBIND11_MODULE(_pymargo, m)
{
//    py11::class_<pymargo_exception>(m, "MargoException")
//        .def_readonly("code", &pymargo_exception::code);
    py11::register_exception<pymargo_exception>(m, "MargoException");
    m.def("request_wait", [](pymargo_request req) {
        auto ret = margo_wait(req);
        if(ret != HG_SUCCESS) throw pymargo_exception("margo_wait", ret);
    });
    m.def("request_test", [](pymargo_request req) {
        int flag;
        auto ret = margo_test(req, &flag);
        if(ret != HG_SUCCESS) throw pymargo_exception("margo_test", (hg_return_t)ret);
        return static_cast<bool>(flag);
    });

    py11::enum_<margo_log_level>(m, "log_level")
        .value("external", MARGO_LOG_EXTERNAL)
        .value("trace", MARGO_LOG_TRACE)
        .value("debug", MARGO_LOG_DEBUG)
        .value("info", MARGO_LOG_INFO)
        .value("warning", MARGO_LOG_WARNING)
        .value("error", MARGO_LOG_ERROR)
        .value("critical", MARGO_LOG_CRITICAL)
        ;
    m.def("set_global_logger", pymargo_set_global_logger)
     .def("set_logger", pymargo_set_logger)
     .def("set_global_log_level", margo_set_global_log_level)
     .def("set_log_level", pymargo_set_log_level)
     .def("trace", (void(*)(const char*))pymargo_trace, "message"_a)
     .def("trace", (void(*)(const char*, pymargo_instance_id))pymargo_trace, "message"_a, "mid"_a)
     .def("debug", (void(*)(const char*))pymargo_debug, "message"_a)
     .def("debug", (void(*)(const char*, pymargo_instance_id))pymargo_debug, "message"_a, "mid"_a)
     .def("info", (void(*)(const char*))pymargo_info, "message"_a)
     .def("info", (void(*)(const char*, pymargo_instance_id))pymargo_info, "message"_a, "mid"_a)
     .def("warning", (void(*)(const char*))pymargo_warning, "message"_a)
     .def("warning", (void(*)(const char*, pymargo_instance_id))pymargo_warning, "message"_a, "mid"_a)
     .def("error", (void(*)(const char*))pymargo_error, "message"_a)
     .def("error", (void(*)(const char*, pymargo_instance_id))pymargo_error, "message"_a, "mid"_a)
     .def("critical", (void(*)(const char*))pymargo_critical, "message"_a)
     .def("critical", (void(*)(const char*, pymargo_instance_id))pymargo_critical, "message"_a, "mid"_a)
     ;

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
        .def("_forward", &pymargo_hg_handle::forward,
             "provider_id"_a=0, "input"_a=py11::bytes(), "timeout"_a=0.0)
        .def("_iforward", &pymargo_hg_handle::iforward,
             "provider_id"_a=0, "input"_a=py11::bytes(), "timeout"_a=0.0)
        .def("_get_output", &pymargo_hg_handle::_get_output)
        .def("_respond", &pymargo_hg_handle::respond)
        .def("_irespond", &pymargo_hg_handle::irespond)
        .def("_get_mid", &pymargo_hg_handle::_get_mid)
        ;

    m.def("init",                     &pymargo_init);
    m.def("finalize", [](pymargo_instance_id mid) {
        margo_finalize(mid);
    });
    m.def("wait_for_finalize",  [](pymargo_instance_id mid) {
        py11::gil_scoped_release release;
        margo_wait_for_finalize(mid);
    });
    m.def("is_listening", [](pymargo_instance_id mid) {
        return static_cast<bool>(margo_is_listening(mid));
    });

    m.def("push_finalize_callback", &pymargo_push_finalize_callback);
    m.def("push_prefinalize_callback", &pymargo_push_prefinalize_callback);
    m.def("enable_remote_shutdown", [](pymargo_instance_id mid) {
            margo_enable_remote_shutdown(mid);
    });
    m.def("shutdown_remote_instance", [](pymargo_instance_id mid, pymargo_addr addr) {
            py11::gil_scoped_release release;
            margo_shutdown_remote_instance(mid, addr);
    });
    m.def("get_config", pymargo_get_config);
    m.def("register",                 &pymargo_register);
    m.def("register_on_client",       &pymargo_register_on_client);
    m.def("registered",               &pymargo_registered);
    m.def("registered_provider",      &pymargo_provider_registered);
    m.def("deregister",               &pymargo_deregister);
    m.def("disable_response",         &pymargo_disable_response);
    m.def("disabled_response",        &pymargo_disabled_response);
    m.def("lookup",                   &pymargo_lookup);
    m.def("addr_free",                &pymargo_addr_free);
    m.def("addr_self",                &pymargo_addr_self);
    m.def("addr_dup",                 &pymargo_addr_dup);
    m.def("addr_cmp",                 &pymargo_addr_cmp);
    m.def("addr_set_remove",          &pymargo_addr_set_remove);
    m.def("addr2str",                 &pymargo_addr_to_string);
    m.def("create",                   &pymargo_create);

    m.def("bulk_create",              &pymargo_bulk_create);
    m.def("bulk_free",                &pymargo_bulk_free);
    m.def("bulk_ref_incr",            &pymargo_bulk_ref_incr);
    m.def("bulk_transfer",            &pymargo_bulk_transfer);
    m.def("bulk_itransfer",           &pymargo_bulk_itransfer);
    m.def("bulk_to_str",              &pymargo_bulk_to_str);
    m.def("str_to_bulk",              &pymargo_str_to_bulk);

    m.def("sleep",                    &pymargo_thread_sleep);

    // Inside abt package
    py11::module abt_package = m.def_submodule("abt");
    abt_package.def("_yield", &py_abt_yield);
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
