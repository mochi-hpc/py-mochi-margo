import sys
import numpy as np
import pymargo
from pymargo.core import Engine
import pymargo.bulk as bulk

def call_rpc_on(engine, rpc_id, addr_str, provider_id, array_str):
    addr = engine.lookup(addr_str)
    handle = engine.create_handle(addr, rpc_id)
    return handle.forward(provider_id, array_str)

with Engine('tcp', mode=pymargo.client) as engine:
    rpc_id = engine.register("send_array")
    myArray = np.random.rand(5,7)
    print(myArray)
    blk = engine.create_bulk(myArray, bulk.read_write)
    print("bulk created successfuly")
    s = blk.to_base64()
    print("bulk converted to base64: " + s)
    call_rpc_on(engine, rpc_id, sys.argv[1], int(sys.argv[2]), s)
