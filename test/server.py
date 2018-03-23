from pymargo import MargoInstance
from pymargo import Provider

class HelloProvider(Provider):

	def __init__(self, mid, mplex_id):
		super(HelloProvider, self).__init__(mid, mplex_id)
		self.register("say_hello", "hello")

	def hello(self, handle, name):
		print "Hello from "+name
		handle.respond("Hi "+name+"!")
		self.get_margo_instance().finalize()

def WhenFinalize():
	print "Finalize was called"

mid = MargoInstance('tcp')
mplex_id = 42
print "Server running at address "+str(mid.addr())+"with mplex_id="+str(mplex_id)

mid.on_finalize(WhenFinalize)
provider = HelloProvider(mid, mplex_id)

mid.wait_for_finalize()
