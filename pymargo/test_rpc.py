import sys
import unittest
from pymargo.core import Engine, Address, Handle

class Receiver():

    def hello_world(self, handle, name):
        handle.respond('Hello '+name)

class TestRPC(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._receiver = Receiver()
        cls._engine = Engine('tcp://127.0.0.1:1234')
        cls._rpc_id = cls._engine.register('hello_world', cls._receiver, 'hello_world')

    @classmethod
    def tearDownClass(cls):
        cls._engine.finalize()

    def test_registered(self):
        self.assertTrue(TestRPC._engine.registered('hello_world'))
        self.assertFalse(TestRPC._engine.registered('other'))

    def test_lookup(self):
        addr = TestRPC._engine.lookup('tcp://127.0.0.1:1234')
        self.assertIsInstance(addr, Address)

    def test_create_handle(self):
        addr = TestRPC._engine.lookup('tcp://127.0.0.1:1234')
        handle = TestRPC._engine.create_handle(addr, TestRPC._rpc_id)
        self.assertIsInstance(handle, Handle)

    def test_forward(self):
        addr = TestRPC._engine.lookup('tcp://127.0.0.1:1234')
        handle = TestRPC._engine.create_handle(addr, TestRPC._rpc_id)
        resp = handle.forward(0, 'Matthieu')
        self.assertEqual(resp, 'Hello Matthieu')

if __name__ == '__main__':
    unittest.main()
