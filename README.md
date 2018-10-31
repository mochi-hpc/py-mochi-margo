# Py-Margo

Py-Margo provides a Python wrapper on top of [Margo](https://xgitlab.cels.anl.gov/sds/margo).
It enables one to develop Margo-based service in Python.

## Dependencies

* margo (and its dependencies)
* python
* pybind11

## Installing

The easiest way to install Py-Margo is to use [spack](https://spack.io/). 
Follow the instructions [here](https://xgitlab.cels.anl.gov/sds/sds-repo) 
to add the `sds` namespace and its packages (instal spack first, if needed).
Then type:

```
spack install py-margo
```

Once installed, you need the py-margo package (and its dependencies) to
be loaded to use it.

## Examples

### Basic example

The following is an example of provider programmed in Python.
Let's put is in a file `server.py`.
The provider listens to an address on a given multiple id (here 42).
Whenever it receives an RPC, it prints "Hello from" and the name sent
by the client, then sends the "Hi "+name+"!" string back to the client,
and finally terminates.

```python
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

mid = MargoInstance('tcp')
provider_id = 42
print "Server running at address "+str(mid.addr())+"with provider_id="+str(provider_id)

provider = HelloProvider(mid, provider_id)

mid.wait_for_finalize()
```

The following code is the corresponding client code (`client.py`).

```python
import sys
import pymargo
from pymargo import MargoInstance

def call_rpc_on(mid, rpc_id, addr_str, provider_id, name):
	addr = mid.lookup(addr_str)
	handle = mid.create_handle(addr, rpc_id, provider_id)
	return handle.forward(provider_id, name)

with MargoInstance('tcp', mode=pymargo.client) as mid:
	rpc_id = mid.register("say_hello")
	ret = call_rpc_on(mid, rpc_id, sys.argv[1], int(sys.argv[2]), sys.argv[3])
	print str(ret)
```

First, run the server on a new terminal:

```
python server.py
```
This will output something like
```
Server running at address ofi+sockets://10.0.2.15:39151 with provider_id=42
```

Then run the client on a new terminal:

```
python client.py ofi+sockets://10.0.2.15:39151 42 Matthieu
```

### Sending/receiving Python objects

The example above shows the basic principles of Py-Margo.
Py-Margo's RPC always use a string as input and respond with a string.
Yet this is sufficient to cover any use-cases you may have: Python
indeed comes with a serialization package, `pickle`, that can take
care of converting almost any Python object from/to a string.

Let us assume we have a file named `mymaths.py` which contains the
following definition of a point in 3D.

```python
class Point():
	def __init__(self,x,y,z):
		self.x = x
		self.y = y
		self.z = z
	def __str__(self):
		return 'Point ('+str(self.x)+','+str(self.y)+','+str(self.z)+')'
```

Then here is a server that can compute a cross product on two points sent by
a client.

```python
from pymargo import MargoInstance
from pymargo import Provider
from mymaths import Point
import pickle

class VectorMathProvider(Provider):

	def __init__(self, mid, provider_id):
		super(VectorMathProvider, self).__init__(mid, provider_id)
		self.register("cross_product", "cross_product")

	def cross_product(self, handle, args):
		points = pickle.loads(args)
		print "Received: "+str(points)
		x = points[0].y*points[1].z - points[0].z*points[1].y
		y = points[0].z*points[1].x - points[0].x*points[1].z
		z = points[0].x*points[1].y - points[0].y*points[1].x
		res = Point(x,y,z)
		handle.respond(pickle.dumps(res))
		self.get_margo_instance().finalize()

mid = MargoInstance('tcp')
provider_id = 42
print "Server running at address "+str(mid.addr())+"with provider_id="+str(provider_id)

provider = VectorMathProvider(mid, provider_id)

mid.wait_for_finalize()
```

And here is a client.

```python
import sys
import pymargo
import pickle
from mymaths import Point
from pymargo import MargoInstance

def call_rpc_on(mid, rpc_id, addr_str, provider_id, p1, p2):
	addr = mid.lookup(addr_str)
	handle = mid.create_handle(addr, rpc_id, provider_id)
	args = pickle.dumps([p1,p2])
	res = handle.forward(args)
	return pickle.loads(res)

with MargoInstance('tcp', mode=pymargo.client) as mid:
	rpc_id = mid.register("cross_product")
	p1 = Point(1,2,3)
	p2 = Point(4,5,6)
	ret = call_rpc_on(mid, rpc_id, sys.argv[1], int(sys.argv[2]), p1, p2)
	print str(ret)
```
