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
        engine = TestAddr.engine
        addr = engine.addr()
        self.assertIsInstance(addr, Address)
        addr = engine.address
        self.assertIsInstance(addr, Address)

    def test_addr_str(self):
        engine = TestAddr.engine
        addr = str(engine.address)
        self.assertIsInstance(addr, str)
        self.assertTrue('na+sm' in addr)

    def test_addr_eql(self):
        engine = TestAddr.engine
        addr = engine.address
        addr_str = str(addr)
        addr_copy = engine.lookup(addr_str)
        self.assertEqual(addr, addr_copy)

    def test_addr_copy(self):
        engine = TestAddr.engine
        addr1 = engine.address
        addr2 = addr1.copy()
        self.assertEqual(addr1, addr2)


if __name__ == '__main__':
    unittest.main()
