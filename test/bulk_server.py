import sys
sys.path.append('build/lib.linux-x86_64-2.7')
import numpy as np
from pymargo.core import Engine, Provider
import pymargo.bulk
from pymargo.bulk import Bulk

class HelloProvider(Provider):

    def __init__(self, mid, provider_id):
        super(HelloProvider, self).__init__(mid, provider_id)
        self.register("send_array", "process_array")

    def process_array(self, handle, bulk_str):
        print("Received send_array RPC")
        print("Bulk string is " + bulk_str)
        engine = self.get_engine()
        localArray = np.random.rand(5,7)
        try:
            localBulk  = engine.create_bulk(localArray, pymargo.bulk.read_write)
            remoteBulk = Bulk.from_base64(engine, bulk_str)
            print("Remote bulk deserialized")
            size = 5 * 7 * localArray.itemsize
            engine.transfer(pymargo.bulk.pull, handle.get_addr(), remoteBulk,
                      0, localBulk, 0, size)
        except Exception as error:
            print("An exception was caught:")
            print(error)
        print("Transfer done")
        print("Received: ")
        print(localArray)
        handle.respond("OK")
        engine.finalize()

def WhenFinalize():
    print "Finalize was called"

mid = Engine('tcp')
provider_id = 42
print "Server running at address " + str(mid.addr()) + " with provider_id " + str(provider_id)

mid.on_finalize(WhenFinalize)
provider = HelloProvider(mid, provider_id)

mid.wait_for_finalize()
