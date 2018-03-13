#define BOOST_NO_AUTO_PTR
#include <boost/python.hpp>
#include <boost/python/return_opaque_pointer.hpp>
#include <boost/python/handle.hpp>
#include <boost/python/enum.hpp>
#include <boost/python/def.hpp>
#include <boost/python/module.hpp>
#include <boost/python/return_value_policy.hpp>
#include <string>
#include <iostream>
#include <mercury_proc_string.h>
#include <margo.h>

BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(margo_instance)
BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(hg_addr)

namespace bpl = boost::python;

struct pymargo_rpc_data {
    bpl::object obj;
    std::string method;
};

struct pymargo_hg_handle {
    hg_handle_t handle;
    pymargo_hg_handle(hg_handle_t h)
        : handle(h) {}
    pymargo_hg_handle(const pymargo_hg_handle& other);
    ~pymargo_hg_handle();
    hg_info get_info() const;
    std::string forward(const std::string& input);
    void respond(const std::string& output);
    margo_instance_id get_mid() const;
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
    try {
        PyObject* pyobj = static_cast<PyObject*>(arg);
        bpl::handle<> h(pyobj); 
        bpl::object fun(h);
        fun(h);
    } catch(const bpl::error_already_set&) {
        PyErr_Print();
        exit(-1);
    }
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

    pymargo_rpc_data* rpc_data = NULL;
    if(info->target_id) {
        void* data = margo_registered_data_mplex(mid, info->id, info->target_id);
        rpc_data = static_cast<pymargo_rpc_data*>(data);
    } else {
        void* data = margo_registered_data(mid, info->id);
        rpc_data = static_cast<pymargo_rpc_data*>(data);
    }

    if(!rpc_data) {
        std::cerr << "[Py-Margo] ERROR: Received a request for a multiplex id "
            << " with no registered provider" << std::endl;
        return HG_OTHER_ERROR;
    }

    std::string out;
    try {
        pymargo_hg_handle pyhandle(handle);
        margo_ref_incr(handle);
        bpl::object fun = rpc_data->obj.attr(rpc_data->method.c_str());
        bpl::object r = 
        fun(pyhandle, std::string(input));
    } catch(const bpl::error_already_set&) {
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
        margo_instance_id mid,
        const std::string& rpc_name,
        uint8_t mplex_id,
        bpl::object obj,
        const std::string& method_name)
{
    int ret;

    hg_id_t rpc_id;
    if(mplex_id) {
        rpc_id = MARGO_REGISTER_MPLEX(mid, rpc_name.c_str(),
                    hg_string_t, hg_string_t, pymargo_generic_rpc_callback,
                    mplex_id, ABT_POOL_NULL);
    } else {
        rpc_id = MARGO_REGISTER(mid, rpc_name.c_str(),
                    hg_string_t, hg_string_t, pymargo_generic_rpc_callback);
    }

    pymargo_rpc_data* rpc_data = new pymargo_rpc_data;
    rpc_data->obj    = obj;
    rpc_data->method = method_name;

    if(mplex_id) {
        ret = margo_register_data_mplex(mid, rpc_id, mplex_id, 
                static_cast<void*>(rpc_data), delete_rpc_data);
    } else {
        ret = margo_register_data(mid, rpc_id,
                static_cast<void*>(rpc_data), delete_rpc_data);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_register_data_mplex() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    return rpc_id;
}

static hg_id_t pymargo_register_on_client(
        margo_instance_id mid,
        const std::string& rpc_name,
        uint8_t mplex_id)
{
    hg_id_t rpc_id;
    if(mplex_id) {
        rpc_id = MARGO_REGISTER_MPLEX(mid, rpc_name.c_str(),
                    hg_string_t, hg_string_t, NULL,
                    mplex_id, ABT_POOL_NULL);
    } else {
        rpc_id = MARGO_REGISTER(mid, rpc_name.c_str(),
                    hg_string_t, hg_string_t, NULL);
    }
    // TODO throw an exception if the return value is not correct
    return rpc_id;
}

static bpl::object pymargo_registered(
        margo_instance_id mid,
        const std::string& rpc_name)
{
    hg_id_t id;
    hg_bool_t flag;
    hg_return_t ret;
    
    ret = margo_registered_name(mid, rpc_name.c_str(), &id, &flag);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_registered_name() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
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
    hg_return_t ret;

    ret = margo_registered_name_mplex(mid, rpc_name.c_str(), mplex_id, &id, &flag);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_registered_name_mplex() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
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
    hg_return_t ret;

    ret = margo_addr_lookup(mid, addrstr.c_str(), &addr);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_addr_lookup() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
    return addr;
}

static void pymargo_addr_free(
        margo_instance_id mid,
        hg_addr_t addr)
{
    hg_return_t ret;
    ret = margo_addr_free(mid, addr);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_addr_free() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
}

static hg_addr_t pymargo_addr_self(
        margo_instance_id mid)
{
    hg_addr_t addr;
    hg_return_t ret;
    ret = margo_addr_self(mid, &addr);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_addr_self() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
    return addr;
}

static hg_addr_t pymargo_addr_dup(
        margo_instance_id mid,
        hg_addr_t addr)
{
    hg_addr_t newaddr;
    hg_return_t ret;
    ret = margo_addr_dup(mid, addr, &newaddr);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_addr_dup() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
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

static pymargo_hg_handle pymargo_create(
        margo_instance_id mid,
        hg_addr_t addr,
        hg_id_t id,
        uint8_t mplex_id)
{
    hg_handle_t handle;
    hg_return_t ret;
    ret = margo_create(mid, addr, id, &handle);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_create() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
    ret = margo_set_target_id(handle, mplex_id);
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_set_target_id() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    // TODO throw an exception if the return value is not  HG_SUCCESS
    pymargo_hg_handle pyhandle(handle);
    return pyhandle;
}

hg_info pymargo_hg_handle::get_info() const
{
    return *margo_get_info(handle);
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

std::string pymargo_hg_handle::forward(const std::string& input)
{
    hg_string_t instr = const_cast<char*>(input.data());
    margo_forward(handle, &instr);

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
    hg_return_t ret = margo_respond(handle, &out);
    // TODO throw an exception if the return value is not  HG_SUCCESS
    if(ret != HG_SUCCESS) {
        std::cerr << "margo_respond() failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
}

margo_instance_id pymargo_hg_handle::get_mid() const
{
    return margo_hg_handle_get_instance(handle);
}

BOOST_PYTHON_MODULE(_pymargo)
{
#define ret_policy_opaque bpl::return_value_policy<bpl::return_opaque_pointer>()

    bpl::opaque<margo_instance>();
    bpl::opaque<hg_addr>();
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
        .def_readonly("target_id", &hg_info::target_id)
        ;
    bpl::class_<pymargo_hg_handle>("MargoHandle", bpl::no_init)
        .def("get_info",&pymargo_hg_handle::get_info)
        .def("forward", &pymargo_hg_handle::forward)
        .def("respond", &pymargo_hg_handle::respond)
        .def("get_mid", &pymargo_hg_handle::get_mid, ret_policy_opaque)
        ;

    bpl::def("init",                    &pymargo_init, ret_policy_opaque);
    bpl::def("finalize",                &margo_finalize);
    bpl::def("wait_for_finalize",       &margo_wait_for_finalize);
    bpl::def("push_finalize_callback",  &pymargo_push_finalize_callback);
    bpl::def("enable_remote_shutdown",  &margo_enable_remote_shutdown);
    bpl::def("register",                &pymargo_register);
    bpl::def("register_on_client",      &pymargo_register_on_client);
    bpl::def("registered",              &pymargo_registered);
    bpl::def("registered_mplex",        &pymargo_registered_mplex);
    bpl::def("lookup",                  &pymargo_lookup, ret_policy_opaque);
    bpl::def("addr_free",               &pymargo_addr_free);
    bpl::def("addr_self",               &pymargo_addr_self, ret_policy_opaque);
    bpl::def("addr_dup",                &pymargo_addr_dup, ret_policy_opaque);
    bpl::def("addr2str",                &pymargo_addr_to_string);
    bpl::def("create",                  &pymargo_create);
//    bpl::def("destroy",                 &pymargo_destroy);
//    bpl::def("ref_incr",                &pymargo_ref_incr);
//    bpl::def("get_info",                &pymargo_get_info);
//    bpl::def("forward",                 &pymargo_forward);
//    bpl::def("forward_timed",           &pymargo_forward_timed);
//    bpl::def("respond",                 &pymargo_respond);
//    bpl::def("handle_get_instance",     &pymargo_hg_handle_get_instance, ret_policy_opaque);

#undef ret_policy_opaque
}
