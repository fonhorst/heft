import random
from deap import tools


class MappingIndividual:
    def __init__(self):
        pass
    pass

def mapping_crossover(child1, child2):
    tools.cxOnePoint(child1, child2)
    pass


def mapping_mutation(mutant):
    k = random.randint(0, len(mutant) - 1)

    pass


class OrderingIndividual:
    def __init__(self):
        pass
    pass

class ResourceConfigIndividual:
    def __init__(self):
        pass
    pass



