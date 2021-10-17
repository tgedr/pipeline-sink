import unittest

from pipelinesink.pipeline_sink import PipelineSinkException


class ExceptionTest(unittest.TestCase):

    def test_exception(self):
        with self.assertRaises(PipelineSinkException) as myex:
            raise PipelineSinkException("oops")
        self.assertEqual("oops", str(myex.exception))