
def run_gapso(gen, n, toolbox, logbook, stats, ga_params, pso_params, ga, pso):

    pop = toolbox.population(n)

    for g in gen:
        for p in pop:
            p.fitness = toolbox.fitness(p)

        (pop, _, _) = pso(toolbox, logbook=None, stats=None, gen_curr=1, gen_step=1, pop=pop, **pso_params)
        (pop, _, _) = ga(toolbox, logbook=None, stats=None, gen_curr=1, gen_step=1, pop=pop, **ga_params)

        data = stats.compile(pop) if stats is not None else None
        if logbook is not None:
            logbook.record(gen=g, evals=len(pop), **data)
            print(logbook.stream)

        for p in pop:
            del p.fitness
        pass

    best = min(pop, key=lambda x: x.fitness)
    return pop, logbook, best



