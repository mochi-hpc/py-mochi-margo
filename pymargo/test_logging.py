import sys
import unittest
from pymargo.core import Engine
from pymargo.logging import Logger
import pymargo.logging

class MyLogger(Logger):

    def __init__(self):
        self._content = {
            "trace": None,
            "debug": None,
            "info": None,
            "warning": None,
            "error": None,
            "critical": None
        }

    @property
    def content(self):
        return self._content

    def trace(self, msg):
        self._content["trace"] = msg

    def debug(self, msg):
        self._content["debug"] = msg

    def info(self, msg):
        self._content["info"] = msg

    def warning(self, msg):
        self._content["warning"] = msg

    def error(self, msg):
        self._content["error"] = msg

    def critical(self, msg):
        self._content["critical"] = msg


class TestLogger(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass
        #cls._engine = Engine('tcp://localhost:1234')

    @classmethod
    def tearDownClass(cls):
        pass
        #cls._engine.finalize()

    def test_global_logger(self):
        logger = MyLogger()
        pymargo.logging.set_global_logger(logger)
        pymargo.logging.set_global_log_level(pymargo.logging.level.info)

        pymargo.logging.trace("AAA")
        pymargo.logging.debug("BBB")
        pymargo.logging.info("CCC")
        pymargo.logging.warning("DDD")
        pymargo.logging.error("EEE")
        pymargo.logging.critical("FFF")

        self.assertIsNone(logger.content['trace'])
        self.assertIsNone(logger.content['debug'])
        self.assertEqual(logger.content['info'], "CCC")
        self.assertEqual(logger.content['warning'], "DDD")
        self.assertEqual(logger.content['error'], "EEE")
        self.assertEqual(logger.content['critical'], "FFF")


if __name__ == '__main__':
    unittest.main()
