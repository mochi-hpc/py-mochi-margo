import sys
import pymargo
from pymargo import MargoInstance

def call_rpc_on(mid, rpc_id, addr_str, mplex_id):
	addr = mid.lookup(addr_str)
	handle = mid.create_handle(addr, rpc_id, mplex_id)
	return handle.forward("Matthieu")

with MargoInstance('tcp', mode=pymargo.client) as mid:
	rpc_id = mid.register("say_hello")
	ret = call_rpc_on(mid, rpc_id, sys.argv[1], int(sys.argv[2]))
	print str(ret)

