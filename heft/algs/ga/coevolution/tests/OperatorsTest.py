import unittest
from heft.algs.ga.coevolution.cga import Env

from heft.core.environment.Utility import wf as WF
from heft.algs.ga.coevolution.operators import check_precedence, ordering_default_mutate


def _topological_sort(wf):
    processed = [wf.head_task.id]
    tasks = wf.get_all_unique_tasks()
    while len(processed) < wf.get_task_count() - 1:
        next_tasks = []
        for t in tasks:
            if all(p.id in processed for p in t.parents):
                processed.append(t.id)
            else:
                next_tasks.append(t)
        tasks = next_tasks
    return processed[1:]




class OperatorsTest(unittest.TestCase):

    def setUp(self):
        wf_name = "Montage_25"
        self.wf = WF(wf_name)
        self.env = Env(self.wf, None, None)

    def test_check_precedence(self):

        ## 21 goes early than 17(its parent)
        seq = ['ID00001_000', 'ID00003_000', 'ID00004_000', 'ID00000_000', 'ID00002_000',
               'ID00007_000', 'ID00008_000', 'ID00012_000', 'ID00006_000', 'ID00005_000',
               'ID00011_000', 'ID00010_000', 'ID00009_000', 'ID00013_000', 'ID00014_000',
               'ID00015_000', 'ID00019_000', 'ID00018_000', 'ID00020_000', 'ID00021_000',
               'ID00017_000', 'ID00016_000', 'ID00024_000', 'ID00022_000', 'ID00023_000']

        assert not check_precedence(self.wf, seq)

    def test_ordering_mutate(self):
        mutant = _topological_sort(self.wf)
        assert check_precedence(self.wf, mutant)

        mutant = ordering_default_mutate({'env': self.env}, mutant)
        assert check_precedence(self.wf, mutant)

    pass
if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(OperatorsTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
