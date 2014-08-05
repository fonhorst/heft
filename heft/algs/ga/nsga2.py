"""
an mapping and/or ordering version of nsga-2
"""
import random


def run_nsga2(toolbox, logbook, stats, gen_curr, gen_step=1, invalidate_fitness=True, pop=None, **params):
    """
    toolbox must contain the following functions:
    population,
    map,
    fitness,
    select,
    clone,
    mutate
    """
    n, CXPB, MU = params['n'], params['crossover_probability'], params['mutation_probability']

    if pop is None:
        pop = toolbox.population(n)

    def calculate_fitness(offsprings):
        invalid_ind = [ind for ind in offsprings if not ind.fitness.valid]
        fitnesses = toolbox.map(toolbox.fitness, invalid_ind)
        for ind, fit in zip(invalid_ind, fitnesses):
            ind.fitness = fit
        return invalid_ind

    invalid_ind = calculate_fitness(pop)
    # This is just to assign the crowding distance to the individuals
    # no actual selection is done
    pop = toolbox.select(pop, len(pop))

    # Begin the generational process
    for gen in range(gen_curr, gen_curr + gen_step):

        record = stats.compile(pop) if stats is not None else {}
        if logbook is not None:
            logbook.record(gen=gen, evals=len(invalid_ind), **record)
            print(logbook.stream)

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
        invalid_ind = calculate_fitness(offspring)

        # Select the next generation population
        pop = toolbox.select(pop + offspring, MU)
        pass

    return pop, logbook, max(pop, key=lambda x: x.fitness)




