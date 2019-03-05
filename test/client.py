import sys
#sys.path.append('build/lib.linux-x86_64-2.7')
import pymargo
from pymargo.core import Engine

def call_rpc_on(mid, rpc_id, addr_str, provider_id, name):
	addr = mid.lookup(addr_str)
	handle = mid.create_handle(addr, rpc_id)
	return handle.forward(provider_id, name)

with Engine('tcp', mode=pymargo.client) as mid:
	rpc_id = mid.register("say_hello")
	ret = call_rpc_on(mid, rpc_id, sys.argv[1], int(sys.argv[2]), str(sys.argv[3]))
	print(str(ret))

