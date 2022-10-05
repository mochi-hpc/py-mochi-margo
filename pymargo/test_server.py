import unittest
import os
from pymargo.core import Engine, on_finalize, on_prefinalize


class MyProvider:

    def __init__(self):
        self._on_finalize_called = False
        self._on_prefinalize_called = False

    @on_finalize
    def finalize(self):
        self._on_finalize_called = True

    @on_prefinalize
    def prefinalize(self):
        self._on_prefinalize_called = True


class TestInitEngine(unittest.TestCase):

    def test_init_engine(self):
        protocol = os.environ.get('MARGO_PROTOCOL', 'na+sm')
        engine = Engine(protocol)
        self.assertIsInstance(engine, Engine)
        self.assertTrue(engine.listening)
        engine.finalize()

    def test_engine_config(self):
        protocol = os.environ.get('MARGO_PROTOCOL', 'na+sm')
        engine = Engine(protocol)
        self.assertIsInstance(engine, Engine)
        config = engine.config
        self.assertIsInstance(config, dict)
        self.assertIn('argobots', config)
        engine.finalize()

    def test_init_engine_fail(self):
        with self.assertRaises(RuntimeError):
            Engine('abc')
        protocol = os.environ.get('MARGO_PROTOCOL', 'na+sm')
        engine = Engine(protocol)
        self.assertIsInstance(engine, Engine)
        engine.finalize()

    class FinalizeCallback():

        def __init__(self):
            self.finalize_was_called = False

        def __call__(self):
            self.finalize_was_called = True

    def test_finalize_callback(self):
        finalize_callback = TestInitEngine.FinalizeCallback()
        self.assertFalse(finalize_callback.finalize_was_called)
        protocol = os.environ.get('MARGO_PROTOCOL', 'na+sm')
        engine = Engine(protocol)
        engine.on_finalize(finalize_callback)
        self.assertFalse(finalize_callback.finalize_was_called)
        engine.finalize()
        self.assertTrue(finalize_callback.finalize_was_called)

    def test_finalize_provider(self):
        protocol = os.environ.get('MARGO_PROTOCOL', 'na+sm')
        engine = Engine(protocol)
        provider = MyProvider()
        engine.register_provider(provider)
        self.assertFalse(provider._on_finalize_called)
        self.assertFalse(provider._on_prefinalize_called)
        engine.finalize()
        self.assertTrue(provider._on_finalize_called)
        self.assertTrue(provider._on_prefinalize_called)


if __name__ == '__main__':
    unittest.main()
