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
        cls.engine = Engine('na+sm')

    @classmethod
    def tearDownClass(cls):
        cls.engine.finalize()

    def test_init_provider(self):
        provider = HelloProvider(TestProvider.engine, 1)
        self.assertIsInstance(provider, HelloProvider)

    def test_provider_get_info(self):
        provider = HelloProvider(TestProvider.engine, 2)
        self.assertEqual(provider.engine, TestProvider.engine)
        self.assertEqual(provider.provider_id, 2)

    def test_provider_forward(self):
        provider = HelloProvider(TestProvider.engine, 3)
        addr = TestProvider.engine.address
        handle = TestProvider.engine.create_handle(addr, provider.rpc_id)
        resp = handle.forward(3, 'Matthieu')
        self.assertEqual(resp, 'Hello Matthieu')

    def test_provider_forward_fail(self):
        provider = HelloProvider(TestProvider.engine, 4)
        addr = TestProvider.engine.address
        handle = TestProvider.engine.create_handle(addr, provider.rpc_id)
        with self.assertRaises(MargoException):
            handle.forward(55, 'Matthieu')


if __name__ == '__main__':
    unittest.main()
