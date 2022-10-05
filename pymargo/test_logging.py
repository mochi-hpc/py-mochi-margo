import unittest
import os
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
        protocol = os.environ.get('MARGO_PROTOCOL', 'na+sm')
        cls.engine = Engine(protocol)

    @classmethod
    def tearDownClass(cls):
        cls.engine.finalize()

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

    def test_instance_logger(self):
        logger = MyLogger()
        self.engine.logger.set_logger(logger)
        self.engine.logger.set_log_level(pymargo.logging.level.info)

        self.engine.logger.trace("AAA")
        self.engine.logger.debug("BBB")
        self.engine.logger.info("CCC")
        self.engine.logger.warning("DDD")
        self.engine.logger.error("EEE")
        self.engine.logger.critical("FFF")

        self.assertIsNone(logger.content['trace'])
        self.assertIsNone(logger.content['debug'])
        self.assertEqual(logger.content['info'], "CCC")
        self.assertEqual(logger.content['warning'], "DDD")
        self.assertEqual(logger.content['error'], "EEE")
        self.assertEqual(logger.content['critical'], "FFF")


if __name__ == '__main__':
    unittest.main()
