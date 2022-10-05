import unittest
import os
from pymargo.core import Engine
import pymargo.bulk
from pymargo.bulk import Bulk


class Receiver():

    def __init__(self, engine):
        self.engine = engine

    def pull_from_bulk(self, handle, bulk, size):
        local_data = bytes(size)
        local_bulk = self.engine.create_bulk(
            local_data, pymargo.bulk.write_only)
        self.engine.transfer(
            op=pymargo.bulk.pull,
            origin_addr=handle.address,
            origin_handle=bulk,
            origin_offset=0,
            local_handle=local_bulk,
            local_offset=0,
            size=size)
        handle.respond(local_data == b'This is some bytes data')

    def push_to_bulk(self, handle, bulk, size):
        local_data = b'more'
        local_bulk = self.engine.create_bulk(
            local_data, pymargo.bulk.read_only)
        self.engine.transfer(
            op=pymargo.bulk.push,
            origin_addr=handle.address,
            origin_handle=bulk,
            origin_offset=8,
            local_handle=local_bulk,
            local_offset=0,
            size=4)
        handle.respond()

    def ipull_from_bulk(self, handle, bulk, size):
        local_data = bytes(size)
        local_bulk = self.engine.create_bulk(
            local_data, pymargo.bulk.write_only)
        self.engine.transfer(
            op=pymargo.bulk.pull,
            origin_addr=handle.address,
            origin_handle=bulk,
            origin_offset=0,
            local_handle=local_bulk,
            local_offset=0,
            size=size,
            blocking=False).wait()
        handle.respond(local_data == b'This is some bytes data')

    def ipush_to_bulk(self, handle, bulk, size):
        local_data = b'more'
        local_bulk = self.engine.create_bulk(
            local_data, pymargo.bulk.read_only)
        self.engine.transfer(
            op=pymargo.bulk.push,
            origin_addr=handle.address,
            origin_handle=bulk,
            origin_offset=8,
            local_handle=local_bulk,
            local_offset=0,
            size=4,
            blocking=False).wait()
        handle.respond()


class TestBulk(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        protocol = os.environ.get('MARGO_PROTOCOL', 'na+sm')
        cls.engine = Engine(protocol)
        cls.receiver = Receiver(cls.engine)
        cls.pull_from_bulk = cls.engine.register(
            'pull_from_bulk', cls.receiver.pull_from_bulk)
        cls.push_to_bulk = cls.engine.register(
            'push_to_bulk', cls.receiver.push_to_bulk)
        cls.ipull_from_bulk = cls.engine.register(
            'ipull_from_bulk', cls.receiver.ipull_from_bulk)
        cls.ipush_to_bulk = cls.engine.register(
            'ipush_to_bulk', cls.receiver.ipush_to_bulk)

    @classmethod
    def tearDownClass(cls):
        cls.engine.finalize()

    def test_register_bulk(self):
        data = b'This is some bytes data'
        bulk = TestBulk.engine.create_bulk(data, pymargo.bulk.read_only)
        self.assertIsInstance(bulk, Bulk)

    def test_pull(self):
        engine = TestBulk.engine
        pull_from_bulk = TestBulk.pull_from_bulk
        addr = engine.address
        data = b'This is some bytes data'
        bulk = TestBulk.engine.create_bulk(data, pymargo.bulk.read_only)
        rpc = pull_from_bulk.on(addr)
        resp = rpc(bulk=bulk, size=len(data))
        self.assertEqual(resp, True)

    def test_push(self):
        engine = TestBulk.engine
        push_to_bulk = TestBulk.push_to_bulk
        addr = engine.address
        data = b'This is some bytes data'
        bulk = TestBulk.engine.create_bulk(data, pymargo.bulk.write_only)
        rpc = push_to_bulk.on(addr)
        rpc(bulk=bulk, size=len(data))
        self.assertEqual(data, b'This is more bytes data')

    def test_ipull(self):
        engine = TestBulk.engine
        pull_from_bulk = TestBulk.ipull_from_bulk
        addr = engine.address
        data = b'This is some bytes data'
        bulk = TestBulk.engine.create_bulk(data, pymargo.bulk.read_only)
        rpc = pull_from_bulk.on(addr)
        resp = rpc(bulk=bulk, size=len(data))
        self.assertEqual(resp, True)

    def test_ipush(self):
        engine = TestBulk.engine
        push_to_bulk = TestBulk.ipush_to_bulk
        addr = engine.address
        data = b'This is some bytes data'
        bulk = TestBulk.engine.create_bulk(data, pymargo.bulk.write_only)
        rpc = push_to_bulk.on(addr)
        rpc(bulk=bulk, size=len(data))
        self.assertEqual(data, b'This is more bytes data')


if __name__ == '__main__':
    unittest.main()
