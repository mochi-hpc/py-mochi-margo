import sys
sys.path.append('build/lib.linux-x86_64-2.7')
from pymargo.core import Engine, Provider

class HelloProvider(Provider):

    def __init__(self, mid, provider_id):
        super(HelloProvider, self).__init__(mid, provider_id)
        self.register("say_hello", "hello")

    def hello(self, handle, name):
        print "Hello from "+name
        print "RPC id is "+str(handle.get_id())
        handle.respond("Hi "+name+"!")
        self.get_margo_instance().finalize()

def WhenFinalize():
    print "Finalize was called"

mid = Engine('tcp')
provider_id = 42
print "Server running at address " + str(mid.addr()) + " with provider_id " + str(provider_id)

mid.on_finalize(WhenFinalize)
provider = HelloProvider(mid, provider_id)

mid.wait_for_finalize()
