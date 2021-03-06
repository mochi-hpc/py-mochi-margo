import sys
import unittest
from pymargo.core import Engine

class TestInitEngine(unittest.TestCase):

    def test_init_engine(self):
        engine = Engine('tcp')
        self.assertIsInstance(engine, Engine)
        engine.finalize()

    def test_init_engine_fail(self):
        with self.assertRaises(RuntimeError):
            engine = Engine('abc')

    class FinalizeCallback():

        def __init__(self):
            self.finalize_was_called = False

        def __call__(self):
            self.finalize_was_called = True

    def test_on_finalize(self):
        finalize_callback = TestInitEngine.FinalizeCallback()
        self.assertFalse(finalize_callback.finalize_was_called)
        engine = Engine('tcp')
        engine.on_finalize(finalize_callback)
        self.assertFalse(finalize_callback.finalize_was_called)
        engine.finalize()
        self.assertTrue(finalize_callback.finalize_was_called)
        

if __name__ == '__main__':
    unittest.main()
