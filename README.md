# Py-Margo

Py-Margo provides a Python wrapper on top of [Margo](https://xgitlab.cels.anl.gov/sds/margo).
It enables one to develop Margo-based service in Python.

## Dependencies

* margo (and its dependencies)
* python
* pybind11
* py-pkgconfig

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
```

The following code is the corresponding client code (`client.py`).

```python
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
from pymargo.core import Engine
from pymargo.core import Provider
from mymaths import Point
import pickle

class VectorMathProvider(Provider):

	def __init__(self, engine, provider_id):
		super().__init__(engine, provider_id)
		self.register("cross_product", "cross_product")

	def cross_product(self, handle, args):
		points = pickle.loads(args)
		print("Received: "+str(points))
		x = points[0].y*points[1].z - points[0].z*points[1].y
		y = points[0].z*points[1].x - points[0].x*points[1].z
		z = points[0].x*points[1].y - points[0].y*points[1].x
		res = Point(x,y,z)
		handle.respond(pickle.dumps(res))
		self.get_engine().finalize()

engine = Engine('tcp')
provider_id = 42
print("Server running at address "+str(mid.addr())+"with provider_id="+str(provider_id))

provider = VectorMathProvider(engine, provider_id)

engine.wait_for_finalize()
```

And here is a client.

```python
import sys
import pymargo
import pickle
from mymaths import Point
from pymargo.core import Engine

def call_rpc_on(engine, rpc_id, addr_str, provider_id, p1, p2):
	addr = engine.lookup(addr_str)
	handle = engine.create_handle(addr, rpc_id, provider_id)
	args = pickle.dumps([p1,p2])
	res = handle.forward(args)
	return pickle.loads(res)

with Engine('tcp', mode=pymargo.client) as engine:
	rpc_id = mid.register("cross_product")
	p1 = Point(1,2,3)
	p2 = Point(4,5,6)
	ret = call_rpc_on(mid, rpc_id, sys.argv[1], int(sys.argv[2]), p1, p2)
	print(str(ret))
```
