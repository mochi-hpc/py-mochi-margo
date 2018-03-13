# Py-Margo

Py-Margo provides a Python wrapper on top of [Margo](https://xgitlab.cels.anl.gov/sds/margo).
It enables one to develop Margo-based service in Python.

## Dependencies

* margo (and its dependencies)
* python
* boost-python

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

	def __init__(self, mid, mplex_id):
		super(HelloProvider, self).__init__(mid, mplex_id)
		self.register("say_hello", "hello")

	def hello(self, handle, name):
		print "Hello from "+name
		handle.respond("Hi "+name+"!")
		self.get_margo_instance().finalize()

mid = MargoInstance('tcp')
mplex_id = 42
print "Server running at address "+str(mid.addr())+"with mplex_id="+str(mplex_id)

provider = HelloProvider(mid, mplex_id)

mid.wait_for_finalize()
```

The following code is the corresponding client code (`client.py`).

```python
import sys
import pymargo
from pymargo import MargoInstance

def call_rpc_on(mid, rpc_id, addr_str, mplex_id, name):
	addr = mid.lookup(addr_str)
	handle = mid.create_handle(addr, rpc_id, mplex_id)
	return handle.forward(name)

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
Server running at address ofi+sockets://10.0.2.15:39151 with mplex_id=42
```

Then run the client on a new terminal:

```
python client.py ofi+sockets://10.0.2.15:39151 42 Matthieu
```

