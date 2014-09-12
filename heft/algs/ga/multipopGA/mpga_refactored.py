from functools import reduce
import operator

from heft.algs.common.utilities import gather_info


def run_mpga(toolbox, logbook, stats, gen_curr=0, gen_step=1, invalidate_fitness=True, initial_populations=None, **params):
    """
    This function signature has been left to correspond standard signature of other algorithms(for example: pso, gsa)
    :param toolbox: contains methods: run_alg, migration
    :param params: must contains - populations, migrCount, generations_count_before_merge, generations_count_after_merge, migrationInterval, is_silent
    :return:
    """

    if initial_populations is None:
        raise ValueError("initial_populations cannot be None")

    populations = initial_populations
    migrCount = params["migrCount"]
    gen_before_merge = params["generations_count_before_merge"]
    gen_after_merge = params["generations_count_after_merge"]
    migrationInterval = params["migrationInterval"]
    is_silent = params["is_silent"]

    gen_isolation_length = int(gen_before_merge / migrationInterval)
    remainings = gen_before_merge % migrationInterval

    best = None

    acquire_best = lambda bst: bst if best is None or best < bst else best

    def merge_populations(populations):
        return reduce(operator.add, populations.values(), [])

    def evolute_and_migrate(populations, gen_curr, gen_step):
        if gen_step == 0:
            return populations
        new_populations = {}
        for name, p in populations.items():
            pop, _, bst = toolbox.run_alg(logbook=None, stats=None, initial_pop=p, gen_curr=gen_curr, gen_step=gen_step)
            new_populations[name] = pop
            best = acquire_best(bst)

        toolbox.migration(new_populations, migrCount)
        gather_info(logbook, stats, g, merge_populations(populations), need_to_print=is_silent)
        return new_populations


    ## process periods with migrations
    for g in range(gen_isolation_length):
        populations = evolute_and_migrate(populations,
                                          gen_curr=g*migrationInterval,
                                          gen_step=migrationInterval)
        pass

    populations = evolute_and_migrate(populations,
                                      gen_curr=gen_isolation_length*migrationInterval,
                                      gen_step=remainings)

    ## process period without migrations and merged population
    whole_population = merge_populations(populations)
    pop, _, bst = toolbox.run_alg(logbook=None, stats=None, initial_pop=whole_population,
                                         gen_curr=gen_isolation_length*migrationInterval + remainings,
                                         gen_step=gen_after_merge)

    best = acquire_best(bst)
    gather_info(logbook, stats, gen_before_merge + gen_after_merge, merge_populations(populations), need_to_print=is_silent)

    return pop, logbook, best




