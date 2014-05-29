class IslandGA:

    def __init__(self):
        pass

    def __call__(self):
        pass

    pass

class IslandAlg:
    def choose_immigrants(self):
        pass
    def choose_emmigrants(self):
        pass
    def bind(self, handler):
        pass
    pass

def topology_scheme(source_island):
    return target_islands

def run_island_ga(**kwargs):
    """
    This algorithm is synchronious, i.e. It is assummed
    that change of generations in all islands performs simultaneously.

    kwargs must contain the following arguments
    :islands = [..., ..., ] - collection of island-algorithm
    every island-algorithm have to support adding of observer function
    to be able to perform interchange between islands
    Observer have to be provided with current population
    (bundle of populations if there are several species)
    and archive if it exists.
    Every island contains emigrants chooser and immigrants chooser
    The first one decides which individuals of population can be
    moved to another islands.
    The second one is responsible for filtering of immigrants going to enter the island.

    :exchange_scheme - alg deciding when every island should send its emigrants.
    """
    pass
