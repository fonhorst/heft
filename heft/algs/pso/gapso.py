
def run_gapso(toolbox, logbook, stats, gen, n,  ga, pso):

    pop = toolbox.population(n)

    best = None

    for g in range(gen):
        for p in pop:
            p.fitness = toolbox.fitness(p)

        b = max(pop, key=lambda x: x.fitness)
        best = max(best, b, key=lambda x: x.fitness) if best is not None else b

        (pop, _, _) = pso(gen_curr=g, gen_step=1, pop=pop, best=best)
        (pop, _, _) = ga(gen_curr=1, gen_step=1, pop=pop)

        data = stats.compile(pop) if stats is not None else None
        if logbook is not None:
            logbook.record(gen=g, evals=len(pop), **data)
            print(logbook.stream)
        pass


    return pop, logbook, best



