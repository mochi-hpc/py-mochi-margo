import unittest
from pymargo.core import Engine, Address, Handle
import _pymargo

class Receiver():

    def hello_world(self, handle, name):
        handle.respond(b'Hello '+name)

    def no_response(self, handle, name):
        pass


class TestRPC(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.receiver = Receiver()
        cls.engine = Engine('na+sm')
        cls.hello_world_id = cls.engine.register(
            'hello_world', cls.receiver, 'hello_world')
        cls.no_response_id = cls.engine.register(
            'no_response', cls.receiver, 'no_response')
        cls.engine.disable_response(cls.no_response_id)
        cls.addr = str(cls.engine.address)

    @classmethod
    def tearDownClass(cls):
        cls.engine.finalize()

    def test_registered(self):
        self.assertTrue(TestRPC.engine.registered('hello_world'))
        self.assertFalse(TestRPC.engine.registered('other'))

    def test_lookup(self):
        addr = TestRPC.engine.lookup(TestRPC.addr)
        self.assertIsInstance(addr, Address)

    def test_create_handle(self):
        addr = TestRPC.engine.lookup(TestRPC.addr)
        handle = TestRPC.engine.create_handle(addr, TestRPC.hello_world_id)
        self.assertIsInstance(handle, Handle)

    def test_forward(self):
        addr = TestRPC.engine.lookup(TestRPC.addr)
        handle = TestRPC.engine.create_handle(addr, TestRPC.hello_world_id)
        resp = handle.forward(0, b'Matthieu')
        self.assertEqual(resp, b'Hello Matthieu')

    def test_forward_no_response(self):
        addr = TestRPC.engine.lookup(TestRPC.addr)
        self.assertTrue(TestRPC.engine.disabled_response(TestRPC.no_response_id))
        handle = TestRPC.engine.create_handle(addr, TestRPC.no_response_id)
        #handle.forward(0, b'Matthieu')

    def test_deregister(self):
        engine = TestRPC.engine
        receiver = Receiver()
        rpc_id = engine.register('something', receiver, 'hello_world')
        self.assertTrue(engine.registered('something'))
        engine.deregister(rpc_id)
        self.assertFalse(engine.registered('something'))

if __name__ == '__main__':
    unittest.main()
