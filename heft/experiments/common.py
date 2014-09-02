from deap import tools
import numpy
from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg



class AbstractExperiment:
    def __init__(self, wf_name,
                 resources_set=[10, 15, 25, 30],
                 ideal_flops=20,
                 transfer_time=100,
                 comp_time_cost=0,
                 transf_time_cost=0):
        self.wf_name = wf_name

        self._resorces_set = resources_set
        self._ideal_flops = ideal_flops
        self._transfer_time = transfer_time
        self._comp_time_cost = comp_time_cost
        self._transf_time_cost =transf_time_cost

        self._wf = None
        self._rm = None
        self._estimator = None

        self._stats = None
        self._logbook = None
        self._toolbox = None

        self._heft_schedule = None

        # TODO: make it config-consuming, i.e
        """
        **config
        self._build_env(config) - a la smart factory method setter
        self.env(self) - a la getter
        """

        pass

    # def _fields(self, private=True, **kwargs):
    #     for k, v in kwargs.items():
    #         name = "_{0}".format(k) if private else "{0}".format(k)
    #         setattr(self, name, v)
    #     pass

    def env(self):
        if not self._wf or not self._rm or not self._estimator:
            self._wf = wf(self.wf_name)
            self._rm = ExperimentResourceManager(rg.r(self._resorces_set))
            self._estimator = SimpleTimeCostEstimator(comp_time_cost=self._comp_time_cost, transf_time_cost=self._transf_time_cost, transferMx=None,
                                            ideal_flops=self._ideal_flops, transfer_time=self._transfer_time)
        return self._wf, self._rm, self._estimator

    def stats(self):
        if self._stats is None:
            self._stats = tools.Statistics(lambda ind: ind.fitness.values[0])
            self._stats.register("avg", numpy.mean)
            self._stats.register("std", numpy.std)
            self._stats.register("min", numpy.min)
            self._stats.register("max", numpy.max)
        return self._stats

    def logbook(self):
        if self._logbook is None:
            self._logbook = tools.Logbook()
            self._logbook.header = ["gen", "evals"] + self.stats().fields
        return self._logbook

    def heft_schedule(self):
        if not self._heft_schedule:
            self._heft_schedule = run_heft(self._wf, self._rm, self._estimator)
        return self._heft_schedule

    def toolbox(self):
        raise NotImplementedError()

    def __call__(self, *args, **kwargs):
        raise NotImplementedError()
    pass
