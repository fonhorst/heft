import cProfile
import pstats
from unittest import TestCase
import io


class ProfilingTestCase(TestCase):

    def test_profiling(self):
        pr = cProfile.Profile()
        pr.enable()
        self.wrap()
        pr.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        self.assertTrue(True)

    def wrap(self):
        pass