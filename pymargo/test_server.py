import unittest
from pymargo.core import Engine


class TestInitEngine(unittest.TestCase):

    def test_init_engine(self):
        engine = Engine('na+sm')
        self.assertIsInstance(engine, Engine)
        self.assertTrue(engine.listening)
        engine.finalize()

    def test_init_engine_fail(self):
        with self.assertRaises(RuntimeError):
            Engine('abc')

    class FinalizeCallback():

        def __init__(self):
            self.finalize_was_called = False

        def __call__(self):
            self.finalize_was_called = True

    def test_on_finalize(self):
        finalize_callback = TestInitEngine.FinalizeCallback()
        self.assertFalse(finalize_callback.finalize_was_called)
        engine = Engine('na+sm')
        engine.on_finalize(finalize_callback)
        self.assertFalse(finalize_callback.finalize_was_called)
        engine.finalize()
        self.assertTrue(finalize_callback.finalize_was_called)


if __name__ == '__main__':
    unittest.main()
