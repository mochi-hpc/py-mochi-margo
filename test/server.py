from pymargo import MargoInstance
from pymargo import Provider

class HelloProvider(Provider):

	def __init__(self, mid, provider_id):
		super(HelloProvider, self).__init__(mid, provider_id)
		self.register("say_hello", "hello")

	def hello(self, handle, name):
		print "Hello from "+name
		handle.respond("Hi "+name+"!")
		self.get_margo_instance().finalize()

def WhenFinalize():
	print "Finalize was called"

mid = MargoInstance('tcp')
provider_id = 42
print "Server running at address "+str(mid.addr())+"with provider_id="+str(provider_id)

mid.on_finalize(WhenFinalize)
provider = HelloProvider(mid, provider_id)

mid.wait_for_finalize()
