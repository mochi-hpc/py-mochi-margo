import unittest
from pymargo.core import Engine, Address


class TestAddr(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = Engine('na+sm')

    @classmethod
    def tearDownClass(cls):
        cls.engine.finalize()

    def test_engine_addr(self):
        addr = TestAddr.engine.addr()
        self.assertIsInstance(addr, Address)
        addr = TestAddr.engine.address
        self.assertIsInstance(addr, Address)

    def test_addr_str(self):
        addr = TestAddr.engine.addr()
        addr_str = str(addr)
        self.assertIsInstance(addr_str, str)
        self.assertTrue('na+sm' in addr_str)

    def test_addr_eql(self):
        addr = TestAddr.engine.addr()
        addr_str = str(addr)
        addr2 = TestAddr.engine.lookup(addr_str)
        self.assertEqual(addr, addr2)


if __name__ == '__main__':
    unittest.main()
