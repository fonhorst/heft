import random

class Specie:
    def __init__(self, name, pop_size, fixed=False, representative_individual=None):
        self.name = name
        self.pop_size = pop_size
        self.fixed = fixed
        self.representative_individual = representative_individual
    pass

def create_cooperative_ga(toolbox):
    def func():
        ## TODO: add ability to determine if evolution has stopped
        ## TODO: add archive
        ## TODO: add generating and collecting differnt statistics
        # create initial populations of all species
        # taking part in coevolution

        SPECIES = toolbox.species
        INTERACT_INDIVIDUALS_COUNT = toolbox.interact_individuals_count
        GENERATIONS = toolbox.generations
        CXB = toolbox.crossover_probability
        MUTATION = toolbox.mutation_probability

        def generate_k(pop):
            base_k = int(INTERACT_INDIVIDUALS_COUNT / len(pop))
            free_slots = INTERACT_INDIVIDUALS_COUNT % len(pop)
            for ind in pop:
                ind.k = base_k

            for slot in free_slots:
                i = random.randint(0, len(pop) - 1)
                pop[i].k += 1
            return pop

        def decrease_k(ind):
            ind.k -= 1
            return ind

        def credit_to_k(pop):
            norma = INTERACT_INDIVIDUALS_COUNT / sum(el.credit for el in pop)
            for c in pop:
                c.k = int(c.credit * norma)
            left_part = INTERACT_INDIVIDUALS_COUNT - sum(c.k for c in pop)
            sorted_pop = sorted(pop, key=lambda x: x.credit, reverse=True)
            for i in range(left_part):
                sorted_pop[i].k += 1
            return pop

        pops = {s: generate_k(toolbox.initialize(s, s.pop_size)) for s in SPECIES if not s.fixed}

        best = None

        for gen in GENERATIONS:
            solutions = []
            for i in range(INTERACT_INDIVIDUALS_COUNT):
                solution = {s: decrease_k(toolbox.select(pop)) for s, pop in pops if not s.fixed}
                solutions.append(solution)

            for sol in solutions:
                sol.fitness = toolbox.fitness(solution)

            inds_credit = dict()
            for sol in solutions:
                for s, ind in sol.items():
                    values = inds_credit.get(ind, [0, 0])
                    values[0] += sol.fitness / len(sol)
                    values[1] += 1
                    inds_credit[ind] = values

            ## TODO: remake it in a more generic way
            ## TODO: add archive and corresponding updating and mixing
            best = min(solution, key=lambda x: x.fitness)

            for ind, values in inds_credit.items():
                ind.credit = values[0] / values[1]

            items = [(s, pop) for s, pop in pops.items() if not s.fixed]
            for s, pop in items:
                offspring = toolbox.select(s, pop)
                offspring = list(map(toolbox.clone, offspring))
                # Apply crossover and mutation on the offspring
                for child1, child2 in zip(offspring[::2], offspring[1::2]):
                    if random.random() < CXB[s]:
                        c1 = child1.credit
                        c2 = child2.credit
                        toolbox.mate(s, child1, child2)
                        ## TODO: make credit inheritance here
                        ## TODO: toolbox.inherit_credit(pop, child1, child2)
                        ## TODO: perhaps, this operation should be done after all crossovers in the pop
                        ## default implementation
                        child1.credit = (c1 + c2) / 2
                        child2.credit = (c1 + c2) / 2
                        pass
                    pass

                for mutant in offspring:
                    if random.random() < MUTATION[s]:
                        toolbox.mutate(s, mutant)
                    pass
                pass

            for s, pop in pops:
                credit_to_k(pop)
                for ind in pop:
                    del ind.credit
            pass
        return best
    return func

def run_cooperative_ga(toolbox):
    return create_cooperative_ga(toolbox)()