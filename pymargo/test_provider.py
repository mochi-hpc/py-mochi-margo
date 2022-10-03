import unittest
import os
import sys
from pymargo.core import Engine, remote, provider


class MyProvider:

    @remote(rpc_name='my_hello_world')
    def hello_world(self, handle):
        handle.respond('Hello World')

    @remote()
    def hello_someone(self, handle, firstname, lastname):
        handle.respond(f'Hello {firstname} {lastname}')


@provider(service_name='dog')
class MyOtherProvider:

    @remote()
    def bark(self, handle):
        handle.respond('Woof')


@remote(rpc_name='global_hello')
def global_hello_world(handle):
    handle.respond('Global Hello World')


class TestProvider(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        protocol = os.environ.get('MARGO_PROTOCOL', 'na+sm')
        cls.engine = Engine(protocol)

    @classmethod
    def tearDownClass(cls):
        cls.engine.finalize()

    def test_provider_object(self):
        engine = TestProvider.engine
        provider = MyProvider()
        rpcs = engine.register_provider(provider, provider_id=42)
        self.assertIn('my_hello_world', rpcs)
        self.assertIn('hello_someone', rpcs)
        rpcs = engine.register_provider(provider, service_name='abc',
                                        provider_id=43)
        self.assertIn('abc_my_hello_world', rpcs)
        self.assertIn('abc_hello_someone', rpcs)

    def test_provider_module(self):
        engine = TestProvider.engine
        provider = sys.modules[__name__]
        rpcs = engine.register_provider(provider, provider_id=45)
        self.assertIn('global_hello', rpcs)

    def test_decorated_provider(self):
        engine = TestProvider.engine
        provider = MyOtherProvider()
        rpcs = engine.register_provider(provider, provider_id=44)
        self.assertIn('dog_bark', rpcs)


if __name__ == '__main__':
    unittest.main()
