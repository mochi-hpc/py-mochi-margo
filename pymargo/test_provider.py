import sys
import unittest
from pymargo.core import Engine, Provider, MargoException

class HelloProvider(Provider):

    def __init__(self, engine, provider_id):
        super().__init__(engine, provider_id)
        self.rpc_id = self.register('hello_world', 'hello')

    def hello(self, handle, name):
        handle.respond('Hello '+name)

class TestProvider(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._engine = Engine('tcp://127.0.0.1:1234')

    @classmethod
    def tearDownClass(cls):
        cls._engine.finalize()

    def test_init_provider(self):
        provider = HelloProvider(TestProvider._engine, 1)
        self.assertIsInstance(provider, HelloProvider)

    def test_provider_get_info(self):
        provider = HelloProvider(TestProvider._engine, 2)
        self.assertEqual(provider.get_engine(), TestProvider._engine)
        self.assertEqual(provider.get_provider_id(), 2)

    def test_provider_forward(self):
        provider = HelloProvider(TestProvider._engine, 3)
        addr = TestProvider._engine.lookup('tcp://127.0.0.1:1234')
        handle = TestProvider._engine.create_handle(addr, provider.rpc_id)
        resp = handle.forward(3, 'Matthieu')
        self.assertEqual(resp, 'Hello Matthieu')

    def test_provider_forward_fail(self):
        provider = HelloProvider(TestProvider._engine, 4)
        addr = TestProvider._engine.lookup('tcp://127.0.0.1:1234')
        handle = TestProvider._engine.create_handle(addr, provider.rpc_id)
        with self.assertRaises(MargoException):
            resp = handle.forward(55, 'Matthieu')

if __name__ == '__main__':
    unittest.main()
