import sys
from pymargo.core import Engine, Provider

class HelloProvider(Provider):

    def __init__(self, engine, provider_id):
        super(engine, provider_id)
        self.register("say_hello", "hello")

    def hello(self, handle, name):
        print("Hello from "+name)
        print("RPC id is "+str(handle.get_id()))
        handle.respond("Hi "+name+"!")
        self.get_engine().finalize()

def WhenFinalize():
    print("Finalize was called")

engine = Engine('tcp')
provider_id = 42
print("Server running at address " + str(engine.addr()) + " with provider_id " + str(provider_id))

engine.on_finalize(WhenFinalize)
provider = HelloProvider(engine, provider_id)

engine.wait_for_finalize()
