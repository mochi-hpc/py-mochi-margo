from pymargo import MargoInstance
from pymargo import Provider

class MyProvider(Provider):

	def __init__(self, mid, mplex_id):
		super(MyProvider, self).__init__(mid, mplex_id)
		self.register("say_hello", "hello")

	def hello(self, handle, name):
		print "Hello from "+name
		handle.respond("Hi "+name+"!")
		self.get_margo_instance().finalize()

mid = MargoInstance('tcp')
mplex_id = 42
print "Server running at address "+str(mid.addr())+"with mplex_id="+str(mplex_id)

provider = MyProvider(mid, mplex_id)

mid.wait_for_finalize()
