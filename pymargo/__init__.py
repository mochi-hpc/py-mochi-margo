import _pymargo

client = _pymargo.mode.client
server = _pymargo.mode.server

in_caller_thread   = _pymargo.location.in_caller_thread
in_progress_thread = _pymargo.location.in_progress_thread

class MargoInstance():

	def __init__(self, addr, mode, use_progress_thread, rpc_location):
		self.__mid = _pymargo.init(addr, mode, use_progress_thread, rpc_location)

	def __enter__(self):
		return self

	def __exit__(self):
		_pymargo_finalize(self.__mid);

	def wait_for_finalize(self):
		_pymargo.wait_for_finalize(self.__mid)
