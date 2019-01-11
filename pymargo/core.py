# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import _pymargo
from bulk import Bulk

"""
Tags to indicate whether an Engine runs as a server
(can receive RPC requests) or as a client (cannot).
"""
client = _pymargo.mode.client
server = _pymargo.mode.server

"""
Tags to indicate where the RPC handlers should execute.
"""
in_caller_thread   = _pymargo.location.in_caller_thread
in_progress_thread = _pymargo.location.in_progress_thread

"""
Definition of an RPC handle. This class is fully defined
in the C++ side of the library.
"""
Handle = _pymargo.Handle

class Address():
    """
    Address class, represents the network address of an Engine.
    """

    def __init__(self, mid, hg_addr, need_del=True):
        """
        Constructor. This method is not supposed to be called
        directly by users. Users need to call the Engine.lookup
        method to lookup a remote Engine.
        """
        self._mid      = mid
        self._hg_addr  = hg_addr
        self._need_del = need_del

    def __del__(self):
        """
        Destructor. This method will free the underlying hg_addr_t object.
        """
        if(self._need_del):
            _pymargo.addr_free(self._mid, self._hg_addr)

    def __str__(self):
        """
        Converts the address into a string. Note that this is only allowed
        on addresses returned by Engine.get_addr(), not on client addresses
        retrieved from RPC handles.
        """
        return _pymargo.addr2str(self._mid, self._hg_addr)

    def copy(self):
        """
        Copies this Address object.
        """
        return Address(self._mid, _pymargo.addr_dup(self._mid, self._hg_addr))

    def shutdown(self):
        """
        Requests the remote Engine to shutdown.
        The user must have called enable_remote_shutdown on the remote Engine.
        """
        _pymargo.shutdown_remote_instance(self._mid, self._hg_addr)

def __Handler_get_Address(h):
    """
    This function gets the address of a the sender of a Handle.
    """
    mid =  h._get_mid()
    addr = h._get_hg_addr()
    return Address(mid, addr, need_del=False).copy()

"""
Since the Handle class is fully defined in C++, the get_addr function is added this way.
"""
setattr(_pymargo.Handle, "get_addr", __Handler_get_Address)

class Engine():

    def __init__(self, addr, 
            mode=server, 
            use_progress_thread=False,
            rpc_location=in_caller_thread):
        """
        Constructor of the Engine class.
        addr : address of the Engine
        mode : pymargo.core.server or pymargo.core.client
        use_progress_thread : whether to use a progress thread or not
        rpc_location : where to call RPC handles
        """
        self._finalized = True
        self._mid = _pymargo.init(addr, mode, use_progress_thread, rpc_location)
        self._finalized = False

    def __del__(self):
        """
        Destructor. Will call finalize it has not been called yet.
        """
        if not self._finalized:
            _pymargo.finalize(self._mid);

    def __enter__(self):
        """
        This method, together with __exit__, enable the "with" synthax for
        an Engine. For example:

        with Engine('tcp') as engine:
           ...
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        See __enter__.
        """
        pass

    def finalize(self):
        """
        Finalizes the Engine.
        """
        _pymargo.finalize(self._mid);
        self._finalized = True

    def wait_for_finalize(self):
        """
        Wait for the Engine to be finalize.
        """
        _pymargo.wait_for_finalize(self._mid)
        self._finalized = True

    def on_finalize(self, callable_obj):
        """
        Registers a callback (i.e. a function or an object with a __call__ method)
        to be called before this Engine is finalized.
        """
        _pymargo.push_finalize_callback(self._mid, callable_obj)

    def enable_remote_shutdown(self):
        """
        Enables this Engine to be remotely shut down.
        """
        _pymargo.enable_remote_shutdown(self._mid)

    def register(self, rpc_name, obj=None, method_name=None, provider_id=0):
        """
        Registers an RPC handle. If the Engine is a client, the obj, method_name
        and provider_id arguments should be ommited. If the Engine is a server,
        obj should be the object instance from which to call the method represented
        by the method_name string, and provider_id should be the provider id at
        which this object is reachable. The object should inherite from the Provider class.
        rpc_name is the name of the RPC, to be used by clients when sending requests.
        """
        if (obj is None) and (method_name is None):
            return _pymargo.register_on_client(self._mid, rpc_name, provider_id)
        elif (obj is not None) and (method_name is not None):
            return _pymargo.register(self._mid, rpc_name, provider_id, obj, method_name)
        else:
            raise RuntimeError('MargoInstance.register: both method name and object instance should be provided')

    def registered(self, rpc_name, provider_id=None):
        """
        Checks if an RPC with the given name is registered. Returns the corresponding
        RPC id if found, None otherwise. If provider_id is given, the returned RPC id will
        integrate it.
        """
        if mplex_id is None:
            return _pymargo.registered(self._mid, rpc_name) 
        else:
            return _pymargo.registered_mplex(self._mid, rpc_name, provider_id)

    def lookup(self, straddr):
        """
        Looks up a remote Engine's address from a string and return an Address instance.
        """
        hg_addr = _pymargo.lookup(self._mid, straddr)
        return Address(self._mid, hg_addr)

    def addr(self):
        """
        Returns the Engine's address (Address instance).
        """
        hg_addr = _pymargo.addr_self(self._mid)
        return Address(self._mid, hg_addr)

    def create_handle(self, addr, rpc_id):
        """
        Creates an RPC Handle to be sent to a given address.
        """
        h = _pymargo.create(self._mid, addr._hg_addr, rpc_id)
        return h

    def create_bulk(self, array, mode):
        """
        Creates a bulk handle to expose the memory used by the provided numpy array.
        The numpy array's memory must be contiguous.
        mode must be bulk.read_only, bulk.write_only, or bulk.read_write.
        Returns a Bulk object.
        """
        blk = _pymargo.bulk_create(self._mid, array, mode)
        return Bulk(self, blk)

    def transfer(self, op, origin_addr, origin_handle, origin_offset, local_handle, local_offset, size):
        """
        Transfers data between Bulk handles.
        op : bulk.push or bulk.pull
        origin_addr : instance of Address of the origin Bulk handle
        origin_handle : remote bulk handle
        origin_offset : offset at the origin
        local_handle : Bulk handle representing local memory
        local_offset : offset in local memory
        size : size of data to transfer
        """
        _pymargo.bulk_transfer(self._mid, op, origin_addr._hg_addr, origin_handle._hg_bulk,
                origin_offset, local_handle._hg_bulk, local_offset, size)

class Provider(object):
    """
    The Provider class represents an object for which some methods can be called remotely.
    """

    def __init__(self, engine, provider_id):
        """
        Constructor.
        engine : instance of Engine attached to this Provider
        provider_id : id at which this provider is reachable
        """
        self._engine = engine
        self._provider_id = provider_id

    def register(self, rpc_name, method_name):
        """
        Registers one of this object's methods to be used as an RPC handler.
        rpc_name : string to use by clients to identify this RPC
        method_name : string representation of the method to call in this object.
        """
        self._engine.register(rpc_name, self, method_name, self._provider_id)

    def registered(self, rpc_name):
        """
        Checks if an RPC with the provided name has been registered for this provider.
        """
        return self._engine.registered(rpc_name, self._provider_id)

    def get_provider_id(self):
        """
        Returns this provider's id.
        """
        return self._provider_id

    def get_margo_instance(self):
        """
        Deprecated. Use get_engine.
        """
        return self._engine

    def get_engine(self):
        """
        Gets the Engine associated with this Provider.
        """
        return self._engine
