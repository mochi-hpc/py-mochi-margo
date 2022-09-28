import unittest
from pymargo.core import Engine, RemoteFunction, CallableRemoteFunction


class Receiver():

    def hello_world(self, handle, firstname, lastname):
        handle.respond(f'Hello {firstname} {lastname}')


class TestRPC(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.receiver = Receiver()
        cls.engine = Engine('na+sm')
        cls.hello_world = cls.engine.register(
            'hello_world',
            cls.receiver.hello_world)

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
        receiver = Receiver()
        rpc = engine.register('something', receiver.hello_world)
        self.assertIsInstance(engine.registered('something'), RemoteFunction)
        rpc.deregister()
        self.assertIsNone(engine.registered('something'), None)


if __name__ == '__main__':
    unittest.main()
