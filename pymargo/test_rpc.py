import unittest
import os
from pymargo.core import Engine, RemoteFunction, CallableRemoteFunction


class Receiver():

    def __init__(self, engine):
        self.engine = engine

    def hello_world(self, handle, firstname, lastname):
        handle.respond(f'Hello {firstname} {lastname}')

    def timeout_rpc(self, handle, arg):
        print("AAA")
        self.engine.sleep(2000.0)
        print("BBB")
        handle.respond('bla')
        print("CCC")

class TestRPC(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        protocol = os.environ.get('MARGO_PROTOCOL', 'na+sm')
        cls.engine = Engine(protocol)
        cls.receiver = Receiver(cls.engine)
        cls.hello_world = cls.engine.register(
            'hello_world',
            cls.receiver.hello_world)
        cls.timeout_rpc = cls.engine.register(
            'timeout_rpc',
            cls.receiver.timeout_rpc)

    @classmethod
    def tearDownClass(cls):
        cls.engine.finalize()

    def test_registered(self):
        engine = TestRPC.engine
        self.assertIsInstance(engine.registered('hello_world'), RemoteFunction)
        self.assertIsNone(engine.registered('other'))

    def test_callable_remote_function(self):
        engine = TestRPC.engine
        hello_world = TestRPC.hello_world
        addr = engine.address
        rpc = hello_world.on(addr)
        self.assertIsInstance(rpc, CallableRemoteFunction)

    def test_call_rpc(self):
        engine = TestRPC.engine
        hello_world = TestRPC.hello_world
        addr = engine.address
        rpc = hello_world.on(addr)
        rpc('Matthieu', lastname='Dorier')

    def test_deregister(self):
        engine = TestRPC.engine
        receiver = Receiver(engine)
        rpc = engine.register('something', receiver.hello_world)
        self.assertIsInstance(engine.registered('something'), RemoteFunction)
        rpc.deregister()
        self.assertIsNone(engine.registered('something'), None)


if __name__ == '__main__':
    unittest.main()
