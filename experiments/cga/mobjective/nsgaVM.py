#    This file is part of DEAP.
#
#    DEAP is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Lesser General Public License as
#    published by the Free Software Foundation, either version 3 of
#    the License, or (at your option) any later version.
#
#    DEAP is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#    GNU Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with DEAP. If not, see <http://www.gnu.org/licenses/>.

import random
import numpy

from deap import base
from deap import creator
from deap import tools
from scoop import futures
from GA.DEAPGA.coevolution.cga import ListBasedIndividual, Env
from GA.DEAPGA.coevolution.operators import mapping_heft_based_initialize, ordering_heft_based_initialize
from core.concrete_realization import ExperimentResourceManager, ExperimentEstimator
from experiments.cga import wf
from environment.ResourceGenerator import ResourceGenerator as rg
from experiments.cga.mobjective.utility import VMResGen, fitness_makespan_and_cost_map_ord, SimpleTimeCostEstimator
from experiments.cga.utilities.common import extract_ordering_from_ga_file, extract_mapping_from_ga_file
from experiments.cga.utilities.double_chromosome_ga import _mate, _mutate


_wf = wf("Montage_75")
# rm = ExperimentResourceManager(VMResGen.r([10, 15, 25, 30], 4))
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
# estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)
estimator = SimpleTimeCostEstimator(comp_time_cost=10, transf_time_cost=2, transferMx=None, ideal_flops=20, transfer_time=100)
env = Env(_wf, rm, estimator)

pop_size = 200
NGEN = 200
MU = 40
CXPB = 0.9

heft_mapping = extract_mapping_from_ga_file("../../../temp/heft_etalon_full_tr100_m75.json", rm)
heft_ordering = extract_ordering_from_ga_file("../../../temp/heft_etalon_full_tr100_m75.json")


creator.create("FitnessMin", base.Fitness, weights=(-1.0, -1.0))
creator.create("Individual", ListBasedIndividual, typecode='d', fitness=creator.FitnessMin)

toolbox = base.Toolbox()


ctx = {'env': env}
pop_mapping = mapping_heft_based_initialize(ctx, pop_size, heft_mapping, 3)
pop_ordering = ordering_heft_based_initialize(ctx, pop_size, heft_ordering, 3)
init_pop = [creator.Individual(el) for el in zip(pop_mapping, pop_ordering)]

toolbox.register("evaluate", fitness_makespan_and_cost_map_ord, ctx)
toolbox.register("mate", _mate, ctx)
toolbox.register("mutate", _mutate, ctx)
toolbox.register("select", tools.selNSGA2)

# toolbox.register("map", futures.map_as_completed)


def main():


    pop = init_pop

    stats = tools.Statistics(lambda ind: ind.fitness.values)
    stats.register("avg", numpy.mean, axis=0)
    stats.register("std", numpy.std, axis=0)
    stats.register("min", numpy.min, axis=0)
    stats.register("max", numpy.max, axis=0)

    logbook = tools.Logbook()
    logbook.header = "gen", "evals", "std", "min", "avg", "max"

    # Evaluate the individuals with an invalid fitness
    invalid_ind = [ind for ind in pop if not ind.fitness.valid]
    fitnesses = toolbox.map(toolbox.evaluate, invalid_ind)
    for ind, fit in zip(invalid_ind, fitnesses):
        ind.fitness.values = fit

    # This is just to assign the crowding distance to the individuals
    # no actual selection is done
    pop = toolbox.select(pop, len(pop))

    record = stats.compile(pop)
    logbook.record(gen=0, evals=len(invalid_ind), **record)
    print(logbook.stream)

    # Begin the generational process
    for gen in range(1, NGEN):
        # Vary the population
        # offspring = tools.selTournamentDCD(pop, len(pop))
        offspring = toolbox.select(pop, len(pop))
        offspring = [toolbox.clone(ind) for ind in offspring]

        for ind1, ind2 in zip(offspring[::2], offspring[1::2]):
            if random.random() <= CXPB:
                toolbox.mate(ind1, ind2)

            toolbox.mutate(ind1)
            toolbox.mutate(ind2)
            del ind1.fitness.values, ind2.fitness.values

        # Evaluate the individuals with an invalid fitness
        invalid_ind = [ind for ind in offspring if not ind.fitness.valid]
        fitnesses = toolbox.map(toolbox.evaluate, invalid_ind)
        for ind, fit in zip(invalid_ind, fitnesses):
            ind.fitness.values = fit

        # Select the next generation population
        pop = toolbox.select(pop + offspring, MU)
        record = stats.compile(pop)
        logbook.record(gen=gen, evals=len(invalid_ind), **record)
        print(logbook.stream)

    return pop, logbook

if __name__ == "__main__":

    pop, stats = main()
    pop.sort(key=lambda x: x.fitness.values)

    import matplotlib.pyplot as plt
    import numpy

    front = numpy.array([ind.fitness.values for ind in pop])

    plt.scatter(front[:,0], front[:,1], c="b")
    plt.axis("tight")
    plt.show()

