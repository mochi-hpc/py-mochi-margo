import unittest
import sys
from pymargo.core import Engine, remote


class MyProvider:

    @remote(rpc_name='my_hello_world')
    def hello_world(self, handle):
        handle.respond('Hello World')

    @remote()
    def hello_someone(self, handle, firstname, lastname):
        handle.respond(f'Hello {firstname} {lastname}')


@remote(rpc_name='global_hello')
def global_hello_world(handle):
    handle.respond('Global Hello World')


class TestProvider(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = Engine('na+sm')

    @classmethod
    def tearDownClass(cls):
        cls.engine.finalize()

    def test_provider_object(self):
        engine = TestProvider.engine
        provider = MyProvider()
        rpcs = engine.register_provider(provider, provider_id=42)
        self.assertIn('my_hello_world', rpcs)
        self.assertIn('MyProvider.hello_someone', rpcs)

    def test_provider_module(self):
        engine = TestProvider.engine
        provider = sys.modules[__name__]
        rpcs = engine.register_provider(provider, provider_id=43)
        self.assertIn('global_hello', rpcs)


if __name__ == '__main__':
    unittest.main()
