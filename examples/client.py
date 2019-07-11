import sys
import pymargo
from pymargo.core import Engine

def call_rpc_on(engine, rpc_id, addr_str, provider_id, name):
    addr = engine.lookup(addr_str)
    handle = engine.create_handle(addr, rpc_id)
    return handle.forward(provider_id, name)

with Engine('tcp', mode=pymargo.client) as engine:
    rpc_id = engine.register("say_hello")
    ret = call_rpc_on(engine, rpc_id, sys.argv[1], int(sys.argv[2]), str(sys.argv[3]))
    print(str(ret))

