import sys
import unittest
from pymargo.core import Engine, Address

class TestAddr(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._engine = Engine('tcp://localhost:1234')

    @classmethod
    def tearDownClass(cls):
        cls._engine.finalize()

    def test_engine_addr(self):
        addr = TestAddr._engine.addr()
        self.assertIsInstance(addr, Address)

    def test_addr_str(self):
        addr = TestAddr._engine.addr()
        addr_str = str(addr)
        self.assertIsInstance(addr_str, str)
        self.assertTrue(':1234' in addr_str) 

if __name__ == '__main__':
    unittest.main()
