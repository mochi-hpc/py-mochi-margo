#define BOOST_NO_AUTO_PTR
#include <boost/python.hpp>
#include <boost/python/return_opaque_pointer.hpp>
#include <boost/python/handle.hpp>
#include <boost/python/enum.hpp>
#include <boost/python/def.hpp>
#include <boost/python/module.hpp>
#include <boost/python/return_value_policy.hpp>
#include <string>
#include <mercury_proc_string.h>
#include <margo.h>

BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(margo_instance)
BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(hg_addr)
BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(hg_handle)

namespace bpl = boost::python;

struct pymargo_rpc_data {
    bpl::object obj;
    std::string method;
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

static margo_instance_id pymargo_init(
        const std::string& addr,
        pymargo_mode mode,
        bool use_progress_thread,
        pymargo_rpc_mode loc)
{
    int l = loc == PYMARGO_IN_CALLER_THREAD ? 0 : -1;
    if(mode == PYMARGO_CLIENT_MODE) l = 0;
    return margo_init(addr.c_str(), mode, (int)use_progress_thread, l);
}

static void pymargo_generic_finalize_cb(void* arg)
{
    PyObject* pyobj = static_cast<PyObject*>(arg);
    bpl::handle<> h(pyobj); 
    bpl::object fun(h);
    fun(h);
}

static void pymargo_push_finalize_callback(
        margo_instance_id mid,
        bpl::object cb)
{
    Py_INCREF(cb.ptr());
    margo_push_finalize_callback(mid, &pymargo_generic_finalize_cb, static_cast<void*>(cb.ptr()));
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

    void* data = margo_registered_data_mplex(mid, info->id, info->target_id);
    pymargo_rpc_data* rpc_data = static_cast<pymargo_rpc_data*>(data);

    bpl::object fun = rpc_data->obj.attr(rpc_data->method.c_str());
    bpl::object res = fun(std::string(input));
    std::string out = bpl::extract<std::string>(bpl::str(res));
    // TODO exception handling

    ret = margo_free_input(handle, &input);
    if(ret != HG_SUCCESS) result = ret;

    ret = margo_destroy(handle);
    if(ret != HG_SUCCESS) result = ret;

    return result;
}
DEFINE_MARGO_RPC_HANDLER(pymargo_generic_rpc_callback)

static hg_id_t pymargo_register(
        margo_instance_id mid,
        const std::string& rpc_name,
        uint8_t mplex_id,
        bpl::object obj,
        const std::string& method_name)
{
    hg_id_t rpc_id = 
        margo_register_name_mplex(mid,
            rpc_name.c_str(),
            &hg_proc_hg_string_t,
            &hg_proc_hg_string_t,
            &pymargo_generic_rpc_callback,
            mplex_id,
            ABT_POOL_NULL);

    pymargo_rpc_data* rpc_data = new pymargo_rpc_data;
    rpc_data->obj    = obj;
    rpc_data->method = method_name;

    margo_register_data_mplex(mid, rpc_id, mplex_id, 
            static_cast<void*>(rpc_data), delete_rpc_data);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    return rpc_id;
}

static bpl::object pymargo_registered(
        margo_instance_id mid,
        const std::string& rpc_name)
{
    hg_id_t id;
    hg_bool_t flag;
    margo_registered_name(mid, rpc_name.c_str(), &id, &flag);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    if(flag) {
        return bpl::object(id);
    } else {
        return bpl::object();
    }
}

static bpl::object pymargo_registered_mplex(
        margo_instance_id mid,
        const std::string& rpc_name,
        uint8_t mplex_id)
{
    hg_id_t id;
    hg_bool_t flag;
    margo_registered_name_mplex(mid, rpc_name.c_str(), mplex_id, &id, &flag);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    if(flag) {
        return bpl::object(id);
    } else {
        return bpl::object();
    }
}

static hg_addr_t pymargo_lookup(
        margo_instance_id mid,
        const std::string& addrstr)
{
    hg_addr_t addr;
    margo_addr_lookup(mid, addrstr.c_str(), &addr);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    return addr;
}

static void pymargo_addr_free(
        margo_instance_id mid,
        hg_addr_t addr)
{
    margo_addr_free(mid, addr);
    // TODO throw an exception if the return value is not  HG_SUCCESS
}

static hg_addr_t pymargo_addr_self(
        margo_instance_id mid)
{
    hg_addr_t addr;
    margo_addr_self(mid, &addr);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    return addr;
}

static hg_addr_t pymargo_addr_dup(
        margo_instance_id mid,
        hg_addr_t addr)
{
    hg_addr_t newaddr;
    margo_addr_dup(mid, addr, &newaddr);
    return newaddr;
}

static std::string pymargo_addr_to_string(
        margo_instance_id mid,
        hg_addr_t addr)
{
    hg_size_t buf_size = 0;
    margo_addr_to_string(mid, NULL, &buf_size, addr);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    std::string result(buf_size+1,' ');
    buf_size += 1;
    margo_addr_to_string(mid, const_cast<char*>(result.data()), &buf_size, addr);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    return result;
}

static hg_handle_t pymargo_create(
        margo_instance_id mid,
        hg_addr_t addr,
        hg_id_t id,
        uint8_t mplex_id)
{
    hg_handle_t handle;
    margo_create(mid, addr, id, &handle);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    margo_set_target_id(handle, mplex_id);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    return handle;
}

static hg_info pymargo_get_info(
        hg_handle_t handle)
{
    return *margo_get_info(handle);
}

static void pymargo_forward(
        hg_handle_t handle,
        const std::string& input)
{
    margo_forward(handle, const_cast<char*>(input.data()));
    // TODO throw an exception if the return value is not  HG_SUCCESS
}

static void pymargo_forward_timed(
        hg_handle_t handle,
        const std::string& input,
        double timeout_ms)
{
    margo_forward_timed(handle, const_cast<char*>(input.data()), timeout_ms);
    // TODO throw an exception if the return value is not  HG_SUCCESS
}

static void pymargo_respond(
        hg_handle_t handle,
        const std::string& output)
{
    margo_respond(handle, const_cast<char*>(output.data()));
    // TODO throw an exception if the return value is not  HG_SUCCESS
}

BOOST_PYTHON_MODULE(_pymargo)
{
    bpl::enum_<pymargo_mode>("mode")
        .value("client", PYMARGO_CLIENT_MODE)
        .value("server", PYMARGO_SERVER_MODE)
        ;
    bpl::enum_<pymargo_rpc_mode>("location")
        .value("in_caller_thread", PYMARGO_IN_CALLER_THREAD)
        .value("in_progress_thread", PYMARGO_IN_PROGRESS_THREAD)
        ;
    bpl::class_<hg_info>("hg_info", bpl::init<>())
        .def_readonly("addr", &hg_info::addr)
        .def_readonly("id", &hg_info::id)
        .def_readonly("target_id", &hg_info::target_id);
    bpl::def("init", &pymargo_init, bpl::return_value_policy<bpl::return_opaque_pointer>());
    bpl::def("finalize", &margo_finalize);
    bpl::def("wait_for_finalize", &margo_wait_for_finalize);
    bpl::def("push_finalize_callback", &pymargo_push_finalize_callback);
    bpl::def("enable_remote_shutdown", &margo_enable_remote_shutdown);
    bpl::def("register", &pymargo_register);
    bpl::def("registered", &pymargo_registered);
    bpl::def("registered_mplex", &pymargo_registered_mplex);
    //bpl::def("disable_response", &pymargo_disable_response);
    bpl::def("lookup", &pymargo_lookup, bpl::return_value_policy<bpl::return_opaque_pointer>());
    bpl::def("addr_free", &pymargo_addr_free);
    bpl::def("addr_self", &pymargo_addr_self, bpl::return_value_policy<bpl::return_opaque_pointer>());
    bpl::def("addr_dup", &pymargo_addr_dup, bpl::return_value_policy<bpl::return_opaque_pointer>());
    bpl::def("addr2str", &pymargo_addr_to_string);
    bpl::def("create", &pymargo_create, bpl::return_value_policy<bpl::return_opaque_pointer>());
    bpl::def("destroy", &margo_destroy);
    bpl::def("ref_incr", &margo_ref_incr);
    bpl::def("get_info", &pymargo_get_info);
    bpl::def("forward", &pymargo_forward);
    bpl::def("forward_timed", &pymargo_forward_timed);
    bpl::def("respond", &pymargo_respond);
    bpl::def("handle_get_instance", &margo_hg_handle_get_instance,
            bpl::return_value_policy<bpl::return_opaque_pointer>());
}
