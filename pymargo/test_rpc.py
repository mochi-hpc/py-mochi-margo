import unittest
from pymargo.core import Engine, Address, Handle


class Receiver():

    def hello_world(self, handle, name):
        handle.respond('Hello '+name)


class TestRPC(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.receiver = Receiver()
        cls.engine = Engine('na+sm')
        cls.rpc_id = cls.engine.register(
            'hello_world', cls.receiver, 'hello_world')
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
        handle = TestRPC.engine.create_handle(addr, TestRPC.rpc_id)
        self.assertIsInstance(handle, Handle)

    def test_forward(self):
        addr = TestRPC.engine.lookup(TestRPC.addr)
        handle = TestRPC.engine.create_handle(addr, TestRPC.rpc_id)
        resp = handle.forward(0, 'Matthieu')
        self.assertEqual(resp, 'Hello Matthieu')


if __name__ == '__main__':
    unittest.main()
