from GA.DEAPGA.GAImplementation import GAFunctions2, Params
from core.HeftHelper import HeftHelper
from environment.ResourceManager import Scheduler


class GAPlanner(Scheduler):

    def __init__(self, workflow, resource_manager, estimator, params=Params(20, 300, 0.8, 0.5, 0.4, 50)):

        population = params.population
        nodes = list(HeftHelper.to_nodes(resource_manager.get_resources()))

        def compcost(job, agent):
            return estimator.estimate_runtime(job, agent)

        def commcost(ni, nj, A, B):
            return estimator.estimate_transfer_time(A, B, ni, nj)

        ranking = HeftHelper.build_ranking_func(nodes, compcost, commcost)
        sorted_tasks = ranking(workflow)

        self.ga_functions = GAFunctions2(workflow, nodes, sorted_tasks, estimator, population)
        pass

    def run(self, current_cleaned_schedule):
        # make save every generation: generation + best fit
        pass
