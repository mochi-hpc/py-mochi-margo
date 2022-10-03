# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import _pymargo
import types
import json
import pickle
from typing import Type, Callable, Any, List, Mapping, Union, Optional
from .bulk import Bulk
from .logging import Logger


"""
Tags to indicate whether an Engine runs as a server
(can receive RPC requests) or as a client (cannot).
"""
client = _pymargo.mode.client
server = _pymargo.mode.server

"""
Definition of an RPC handle. This class is fully defined
in the C++ side of the library.
"""
Handle = _pymargo.Handle

"""
Exception raised by most of the pymargo C++ functions.
"""
MargoException = _pymargo.MargoException


class Address:
    """
    Address class, represents the network address of an Engine.
    """

    def __init__(self, mid: _pymargo.margo_instance_id,
                 hg_addr: _pymargo.hg_addr_t,
                 need_del: bool = True):
        """
        Constructor. This method is not supposed to be called
        directly by users. Users need to call the Engine.lookup
        method to lookup a remote Engine.
        """
        self._mid = mid
        self._hg_addr = hg_addr
        self._need_del = need_del

    def __del__(self) -> None:
        """
        Destructor. This method will free the underlying hg_addr_t object.
        """
        if self._need_del:
            _pymargo.addr_free(self._mid, self._hg_addr)

    def __str__(self) -> str:
        """
        Converts the address into a string. Note that this is only allowed
        on addresses returned by Engine.get_addr(), not on client addresses
        retrieved from RPC handles.
        """
        return _pymargo.addr2str(self._mid, self._hg_addr)

    def __eq__(self, other: Any) -> bool:
        """
        Checks for equality between two addresses.
        """
        if isinstance(other, Address):
            return _pymargo.addr_cmp(self._mid, self._hg_addr, other._hg_addr)
        else:
            return False

    def copy(self) -> 'Address':
        """
        Copies this Address object.
        """
        return Address(self._mid, _pymargo.addr_dup(self._mid, self._hg_addr))

    def shutdown(self) -> None:
        """
        Requests the remote Engine to shutdown.
        The user must have called enable_remote_shutdown on the remote Engine.
        """
        _pymargo.shutdown_remote_instance(self._mid, self._hg_addr)

    def get_internal_hg_addr(self) -> _pymargo.hg_addr_t:
        """
        Get the internal hg_addr handle.
        """
        return self._hg_addr

    @property
    def hg_addr(self) -> _pymargo.hg_addr_t:
        return self._hg_addr


def __Handle_get_Address(h: _pymargo.Handle) -> Address:
    """
    This function gets the address of a the sender of a Handle.
    """
    mid = h._get_mid()
    addr = h._get_hg_addr()
    return Address(mid, addr, need_del=False).copy()


def __Handle_respond(h: _pymargo.Handle, data: Any = None) -> None:
    """
    This function calls h._respond with pickled data.
    """
    h._respond(pickle.dumps(data))


"""
Since the Handle class is fully defined in C++, the get_addr
and respond functions must added this way to return an Address object
and use the pickle module, respectively
"""
setattr(_pymargo.Handle, "get_addr", __Handle_get_Address)
setattr(_pymargo.Handle, "respond", __Handle_respond)


class Request:
    """
    Generic Request wrapper for non-blocking operations.
    """

    def __init__(self, req: _pymargo.margo_request):
        self._req = req

    def wait(self):
        """
        Wait for the request to complete.
        """
        self._req.wait()

    def test(self):
        """
        Test if the request has completed.
        """
        return self._req.test()


class ForwardRequest(Request):
    """
    A ForwardRequest object wraps a request that has
    been obtained via a forward calls.
    """

    def __init__(self, req: _pymargo.margo_request,
                 handle: _pymargo.Handle):
        super().__init__(req)
        self._handle = handle

    def wait(self):
        """
        Wait for the request to complete and return
        the output of the RPC.
        """
        super().wait()
        return self._handle._get_output()


class CallableRemoteFunction:
    """
    A CallableRemoteFunction object is RemoteFunction that has
    been bound with an Address to become callable. It provides
    a generic __call__ method to act like a function.
    """

    def __init__(self, remote_function: 'RemoteFunction',
                 handle: _pymargo.Handle, provider_id: int):
        self._remote_function = remote_function
        self._handle = handle
        self._provider_id = provider_id

    @property
    def handle(self) -> _pymargo.Handle:
        return self._handle

    @property
    def remote_function(self) -> 'RemoteFunction':
        return self._remote_function

    @property
    def engine(self) -> 'Engine':
        return self.remote_function.engine

    @property
    def provider_id(self) -> int:
        return self._provider_id

    def _forward(self, *args: List[Any], timeout: float = 0.0,
                **kwargs: Mapping[str, Any]) -> Any:
        data = {
            'args': args,
            'kwargs': kwargs
        }
        raw_data = pickle.dumps(data)
        raw_response = self.handle._forward(provider_id=self.provider_id,
                                            input=raw_data, timeout=timeout)
        if raw_response is None:
            return None
        return pickle.loads(raw_response)  # type: ignore

    def _iforward(self, *args: List[Any], timeout: float = 0.0,
                  **kwargs: Mapping[str, Any]) -> Any:
        data = {
            'args': args,
            'kwargs': kwargs
        }
        raw_data = pickle.dumps(data)
        req = self.handle._iforward(provider_id=self.provider_id,
                                    input=raw_data, timeout=timeout)
        return ForwardRequest(req, self._handle)

    def __call__(self, *args: List[Any], timeout: float = 0.0,
                 blocking=True, **kwargs: Mapping[str, Any]) -> Any:
        if blocking:
            return self._forward(*args, timeout=timeout, **kwargs)
        else:
            return self._iforward(*args, timeout=timeout, **kwargs)


class RemoteFunction:
    """
    A RemoteFunction object is a wrapper for an RPC id to
    enable manipulating it in an object-oriented manner.
    """

    def __init__(self, engine: 'Engine', rpc_id: int):
        self._engine = engine
        self._rpc_id = rpc_id

    @property
    def rpc_id(self) -> int:
        return self._rpc_id

    @property
    def engine(self) -> 'Engine':
        return self._engine

    def on(self, address: Address,
           provider_id: int = 0) -> CallableRemoteFunction:
        """
        Binds the RemoteFunction to an Address and provider_id,
        returning a CallableRemoteFunction that can be called.
        """
        handle = _pymargo.create(self.engine.mid, address.hg_addr, self.rpc_id)
        return CallableRemoteFunction(self, handle, provider_id)

    def disable_response(self, disable: bool = True) -> None:
        """
        Disable or enable response for the RemoteFunction.
        """
        _pymargo.disable_response(self.engine.mid, self.rpc_id, disable)

    def disabled_response(self) -> bool:
        """
        Returns whether response is disabled for this RemoteFunction.
        """
        return _pymargo.disabled_response(self.engine.mid, self.rpc_id)

    def deregister(self) -> None:
        """
        Deregisters the function from the engine.
        """
        _pymargo.deregister(self.engine.mid, self.rpc_id)


class Engine:
    """
    The Engine class wraps a margo_instance_id to provide a higher-level
    python interface for registering RPCs, looking up addresses, etc.
    """

    class EngineLogger(Logger):
        """
        The EngineLogger class is used to redirect messages passed
        to engine.logger to the Engine's underlying margo instance.
        """

        def __init__(self, engine: 'Engine'):
            self._engine = engine

        def trace(self, msg: str) -> None:
            _pymargo.trace(msg, mid=self._engine.mid)

        def debug(self, msg: str) -> None:
            _pymargo.debug(msg, mid=self._engine.mid)

        def info(self, msg: str) -> None:
            _pymargo.info(msg, mid=self._engine.mid)

        def warning(self, msg: str) -> None:
            _pymargo.warning(msg, mid=self._engine.mid)

        def error(self, msg: str) -> None:
            _pymargo.error(msg, mid=self._engine.mid)

        def critical(self, msg: str) -> None:
            _pymargo.critical(msg, mid=self._engine.mid)

        def set_logger(self, logger: Logger) -> None:
            """
            Set the Logger object for the underlying margo
            instance to redirect messages to.
            """
            _pymargo.set_logger(self._engine, logger)

        def set_log_level(self, log_level: _pymargo.log_level):
            """
            Set the log level of the underlying margo instance's logger.
            """
            _pymargo.set_log_level(self._engine._mid, log_level)

    def __init__(self, addr: str,
                 mode: _pymargo.mode = server,
                 use_progress_thread: bool = False,
                 num_rpc_threads: int = 0,
                 options: Union[str, dict] = ""):
        """
        Constructor of the Engine class.
        addr : address of the Engine
        mode : pymargo.core.server or pymargo.core.client
        use_progress_thread : whether to use a progress execution stream or not
        num_rpc_threads : Number of RPC execution streams
        options : options dictionary (or serialized in json)
        """
        self._finalized = True
        if isinstance(options, dict):
            opt = json.dumps(options)
        else:
            opt = options
        self._mid = _pymargo.init(
            addr, mode, use_progress_thread, num_rpc_threads, opt)
        self._finalized = False
        self._logger = Engine.EngineLogger(self)

    def __del__(self) -> None:
        """
        Destructor. Will call finalize it has not been called yet.
        """
        if not self._finalized:
            self.finalize()

    def __enter__(self):
        """
        This method, together with __exit__, enable the "with" synthax for
        an Engine. For example:

        with Engine('tcp') as engine:
           ...
        """
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[types.TracebackType]) -> None:
        """
        See __enter__.
        """
        if not self._finalized:
            self.finalize()

    def finalize(self) -> None:
        """
        Finalizes the Engine.
        """
        _pymargo.finalize(self._mid)
        self._finalized = True

    def wait_for_finalize(self) -> None:
        """
        Wait for the Engine to be finalize.
        """
        _pymargo.wait_for_finalize(self._mid)
        self._finalized = True

    @property
    def listening(self) -> bool:
        """
        Returns whether the engine is listening for RPCs.
        """
        return _pymargo.is_listening(self._mid)

    def on_prefinalize(self, callable_obj: Callable[[], None]) -> None:
        """
        Registers a callback (i.e. a function or an object with a
        __call__ method) to be called before the Engine is finalized
        and before the Mercury progress loop is terminated.
        """
        _pymargo.push_prefinalize_callback(self._mid, callable_obj)

    def on_finalize(self, callable_obj: Callable[[], None]) -> None:
        """
        Registers a callback (i.e. a function or an object with
        a __call__ method) to be called before this Engine is finalized.
        """
        _pymargo.push_finalize_callback(self._mid, callable_obj)

    def enable_remote_shutdown(self) -> None:
        """
        Enables this Engine to be remotely shut down.
        """
        _pymargo.enable_remote_shutdown(self._mid)

    def register(self, rpc_name: Optional[str] = None,
                 func: Optional[Callable] = None,
                 provider_id: int = 0,
                 disable_response: bool = False):
        """
        Registers an RPC handle. If the Engine is a client, the function
        and provider_id arguments should be ommited. If the engine is a
        server, function should be a callable object (function or method
        bound to an object. rpc_name is the name of the RPC, to be used
        by clients when sending requests.
        """
        if func is None:
            if rpc_name is None:
                raise ValueError(
                    'Client-side RPC registration needs rpc_name argument')
            rpc_id = _pymargo.register_on_client(
                self._mid, rpc_name, provider_id)

        else:
            if rpc_name is None:
                if hasattr(func, '_pymargo_info'):
                    the_rpc_name = \
                        func._pymargo_info['rpc_name']  # type: ignore
                    disable_response = \
                        func._pymargo_info['disable_response']  # type: ignore
                elif isinstance(func, types.MethodType):
                    the_rpc_name = func.__func__.__qualname__
                elif isinstance(func, types.FunctionType):
                    the_rpc_name = func.__qualname__
            else:
                the_rpc_name = str(rpc_name)

            def wrapper(handle, raw_input_data):
                input_data = pickle.loads(raw_input_data)
                args = input_data['args']
                kwargs = input_data['kwargs']
                func(handle, *args, **kwargs)
            rpc_id = _pymargo.register(
                self._mid, the_rpc_name, provider_id, wrapper)
        remote_function = RemoteFunction(self, rpc_id)
        remote_function.disable_response(disable_response)
        return remote_function

    def register_provider(self, provider: Any, provider_id: int = 0,
                          service_name: Optional[str] = None) \
            -> Mapping[str, RemoteFunction]:
        """
        Discovers all the function decorated with @remote in the
        provider and registers them in this engine with the specified
        provider id.
        """
        provider_cls = type(provider)
        if hasattr(provider_cls, '_pymargo_provider_info'):
            if service_name is None:
                service_name = \
                    provider_cls._pymargo_provider_info['service_name']
        funcs = [getattr(provider, f) for f in dir(provider)]
        funcs = [f for f in funcs if hasattr(f, '_pymargo_info')]
        result = {}
        for func in funcs:
            if service_name is None:
                service_name = func._pymargo_info['service_name']
            rpc_name = func._pymargo_info['rpc_name']
            if service_name is not None:
                rpc_name = service_name + "_" + rpc_name
            result[rpc_name] = self.register(func=func, rpc_name=rpc_name)
        return result

    def registered(self, rpc_name: str,
                   provider_id: Optional[int] = None) \
            -> Optional[RemoteFunction]:
        """
        Checks if an RPC with the given name is registered.
        Returns the corresponding RPC id if found, None otherwise.
        If provider_id is given, the returned RPC id will integrate it.
        """
        if provider_id is None:
            rpc_id = _pymargo.registered(self._mid, rpc_name)
        else:
            rpc_id = _pymargo.registered_provider(
                self._mid, rpc_name, provider_id)
        if rpc_id is None:
            return None
        else:
            return RemoteFunction(self, rpc_id)  # type: ignore

    def lookup(self, straddr: str) -> Address:
        """
        Looks up a remote Engine's address from a string and
        return an Address instance.
        """
        hg_addr = _pymargo.lookup(self._mid, straddr)
        return Address(self._mid, hg_addr)

    def addr(self) -> Address:
        """
        Returns the Engine's address (Address instance).
        """
        hg_addr = _pymargo.addr_self(self._mid)
        return Address(self._mid, hg_addr)

    @property
    def address(self) -> Address:
        """
        Returns the Engine's address (Address instance).
        """
        return self.addr()

    def create_bulk(self, array: Any, mode: _pymargo.access) -> Bulk:
        """
        Creates a bulk handle to expose the memory used by the provided array
        (which can be any python type that satisfies the buffer protocol,
        e.g. a bytearray or a numpy array, for instance).
        The array's memory must be contiguous.
        mode must be bulk.read_only, bulk.write_only, or bulk.read_write.
        Returns a Bulk object.
        """
        blk = _pymargo.bulk_create(self._mid, array, mode)
        return Bulk(self, blk)

    def transfer(self, op: _pymargo.xfer, origin_addr: Address,
                 origin_handle: Bulk, origin_offset: int,
                 local_handle: Bulk, local_offset: int, size: int) -> None:
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
        _pymargo.bulk_transfer(
            self._mid, op, origin_addr._hg_addr, origin_handle._hg_bulk,
            origin_offset, local_handle._hg_bulk, local_offset, size)

    def get_internal_mid(self) -> _pymargo.margo_instance_id:
        """
        Returns the internal margo_instance_id.
        """
        return self._mid

    @property
    def mid(self) -> _pymargo.margo_instance_id:
        """
        Returns the internal margo_instance_id.
        """
        return self._mid

    @property
    def logger(self) -> EngineLogger:
        """
        Returns a Logger instance that will redirect messages to the
        internal Margo instance.
        """
        return self._logger

    def set_remove(self, address: Address) -> None:
        """
        Hint that the address is no longer valid.
        """
        _pymargo.addr_set_remove(self._mid, address.hg_addr)

    def sleep(self, delay: float) -> None:
        """
        Sleep for the specified number of seconds,
        yielding to other Argobots threads.
        """
        _pymargo.sleep(self._mid, delay)

    @property
    def config(self) -> dict:
        config_str = _pymargo.get_config(self._mid)
        if config_str is None:
            return None
        import json
        return json.loads(config_str)


def remote(rpc_name: Optional[str] = None,
           disable_response: Optional[bool] = False,
           service_name: Optional[str] = None):
    """
    Decorator that adds information to a function to tell
    pymargo how it should be registeted as an RPC.
    """
    def decorator(func):
        name = rpc_name
        sname = service_name
        if name is None:
            if isinstance(func, types.MethodType):
                name = func.__func__.__name__
            elif isinstance(func, types.FunctionType):
                name = func.__name__
        func._pymargo_info = {
            'rpc_name': name,
            'disable_response': disable_response,
            'service_name': service_name
        }
        return func
    return decorator


def provider(service_name: Optional[str] = None):
    """
    Decorator that adds information to a class to tell
    pymargo how it should be registered as a provider.
    """
    def decorator(cls):
        _pymargo_info = {
            'service_name': service_name
        }
        setattr(cls, '_pymargo_provider_info', _pymargo_info)
        return cls
    return decorator
