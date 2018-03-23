# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
import _pymargo

client = _pymargo.mode.client
server = _pymargo.mode.server

in_caller_thread   = _pymargo.location.in_caller_thread
in_progress_thread = _pymargo.location.in_progress_thread

class MargoAddress():

	def __init__(self, mid, hg_addr, need_del=True):
		self._mid      = mid
		self._hg_addr  = hg_addr
		self._need_del = need_del

	def __del__(self):
		_pymargo.addr_free(self._mid, self._hg_addr)

	def __str__(self):
		return _pymargo.addr2str(self._mid, self._hg_addr)

	def copy(self):
		return MargoAddress(self._mid, _pymargo.addr_dup(self._mid, self._hg_addr))

	def get_hg_addr(self):
		return self._hg_addr

class MargoInstance():

	def __init__(self, addr, 
			mode=server, 
			use_progress_thread=False,
			rpc_location=in_caller_thread):
		self._mid = _pymargo.init(addr, mode, use_progress_thread, rpc_location)
		self._finalized = False

	def __del__(self):
		if not self._finalized:
			_pymargo.finalize(self._mid);

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		pass

	def finalize(self):
		_pymargo.finalize(self._mid);
		self._finalized = True

	def wait_for_finalize(self):
		_pymargo.wait_for_finalize(self._mid)
		self._finalized = True

	def on_finalize(self, callable_obj):
		_pymargo.push_finalize_callback(self._mid, callable_obj)

	def enable_remote_shutdown(self):
		_pymargo.enable_remote_shutdown(self._mid)

	def register(self, rpc_name, obj=None, method_name=None, mplex_id=0):
		if (obj is None) and (method_name is None):
			return _pymargo.register_on_client(self._mid, rpc_name, mplex_id)
		elif (obj is not None) and (method_name is not None):
			return _pymargo.register(self._mid, rpc_name, mplex_id, obj, method_name)
		else:
			raise RuntimeError('MargoInstance.register: both method name and object instance should be provided')

	def registered(self, rpc_name, mplex_id=None):
		if mplex_id is None:
			return _pymargo.registered(self._mid, rpc_name) 
		else:
			return _pymargo.registered_mplex(self._mid, rpc_name, mplex_id)

	def lookup(self, straddr):
		hg_addr = _pymargo.lookup(self._mid, straddr)
		return MargoAddress(self._mid, hg_addr)

	def addr(self):
		hg_addr = _pymargo.addr_self(self._mid)
		return MargoAddress(self._mid, hg_addr)

	def create_handle(self, addr, rpc_id, mplex_id=0):
		return _pymargo.create(self._mid, addr.get_hg_addr(), rpc_id, mplex_id)

class Provider(object):

	def __init__(self, mid, mplex_id):
		self._mid = mid
		self._mplex_id = mplex_id

	def register(self, rpc_name, method_name):
		self._mid.register(rpc_name, self, method_name, self._mplex_id)

	def registered(self, rpc_name):
		return self._mid.registered(rpc_name, self._mplex_id)

	def get_mplex_id(self):
		return self._mplex_id

	def get_margo_instance(self):
		return self._mid

