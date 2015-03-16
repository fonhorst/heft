from copy import deepcopy
import random
from deap.base import Toolbox
import numpy
from heft.algs.pso.ordering_operators import build_schedule, generate, ordering_update, fitness

from heft.algs.pso.sdpso import run_pso
from heft.algs.pso.mapping_operators import update as mapping_update
from heft.core.environment.Utility import Utility
from heft.experiments.aggregate_utilities import interval_statistics, interval_stat_string
from heft.experiments.cga.utilities.common import repeat
from heft.experiments.common import AbstractExperiment
from heft.algs.heft.HeftHelper import HeftHelper

class OmpsoBaseExperiment(AbstractExperiment):

    @staticmethod
    def run(**kwargs):
        inst = OmpsoBaseExperiment(**kwargs)
        return inst()

    def __init__(self, wf_name, W, C1, C2, GEN, N, data_intensive):
        super().__init__(wf_name)

        self.W = W
        self.C1 = C1
        self.C2 = C2
        self.GEN = GEN
        self.N = N
        self.data_intensive = data_intensive
        pass

    def __call__(self):

        stats, logbook = self.stats(), self.logbook()
        _wf, rm, estimator = self.env()


        estimator.transfer_time = self.data_intensive
        toolbox = self.toolbox(self.data_intensive)
        heft_schedule = self.heft_schedule()

        pop, log, best, res_list = run_pso(
            toolbox=toolbox,
            logbook=logbook,
            stats=stats,
            gen_curr=0, gen_step=self.GEN, invalidate_fitness=True, initial_pop=None,
            w=self.W, c1=self.C1, c2=self.C2, n=self.N,
        )

        schedule = build_schedule(_wf, rm, estimator,  best)

        Utility.validate_static_schedule(_wf, schedule)
        makespan = Utility.makespan(schedule)
        #print("Final makespan: {0}".format(makespan))
        #print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))
        return makespan


    def toolbox(self, transfer):

        _wf, rm, estimator = self.env()
        estimator.transfer_time = transfer
        heft_schedule = self.heft_schedule()

        heft_particle = generate(_wf, rm, estimator, heft_schedule)

        heft_gen = lambda n: ([deepcopy(heft_particle) if random.random() > 1.00 else generate(_wf, rm, estimator) for _ in range(n-1)] + [deepcopy(heft_particle)])
        #heft_gen = lambda n: ([deepcopy(heft_particle) if random.random() > 1.00 else generate(_wf, rm, estimator) for _ in range(n)])

        def componoud_update(w, c1, c2, p, best, pop, min=-1, max=1):
            #doMap = random.random()
            #if doMap < 0.5:
            mapping_update(w, c1, c2, p.mapping, best.mapping, pop)
            ordering_update(w, c1, c2, p.ordering, best.ordering, pop, min=min, max=max)

        toolbox = Toolbox()
        toolbox.register("population", heft_gen)
        toolbox.register("fitness", fitness, _wf, rm, estimator)
        toolbox.register("update", componoud_update)
        return toolbox


    pass

def get_data_rate(jobslist):
            jobs_copy = jobslist.copy()
            total_job_rate = 0
            total_runtime = 0
            total_datasize = 0
            for it in range(len(jobs_copy)):
                job = jobs_copy.pop()
                cur_datasize = 0
                for file in job.input_files.items():
                    cur_datasize = cur_datasize + file[1].size
                total_job_rate = total_job_rate + (cur_datasize / job.runtime)
                total_runtime = total_runtime + job.runtime
                total_datasize = total_datasize + cur_datasize
            total_job_rate = total_job_rate / len(jobslist)
            total_runtime = total_runtime / len(jobslist)
            total_datasize = total_datasize / len(jobslist)

            return total_job_rate


if __name__ == "__main__":
    exp = OmpsoBaseExperiment(wf_name="_30",
                              W=0.5, C1=1.6, C2=1.2,
                              GEN=100, N=50, data_intensive=100)
    result = repeat(exp, 8)
    print(result)
    sts = interval_statistics(result)
    print("Statistics: {0}".format(interval_stat_string(sts)))
    print("Average: {0}".format(numpy.mean(result)))
    pass

