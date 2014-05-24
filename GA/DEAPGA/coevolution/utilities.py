import json
from deap import tools


def build_ms_ideal_ind(wf, resource_manager):
    ## TODO: reconsider and make this procedure more stable
    res = []
    i = 0
    _nodes = sorted(resource_manager.get_nodes(), key=lambda x: x.flops)
    for t in sorted(wf.get_all_unique_tasks(), key=lambda x: x.id):
        res.append((t.id, _nodes[i].name))
        i = (i + 1 )%len(_nodes)
    return res

def build_os_ideal_ind(wf):
    ## TODO: reconsider and make this procedure more stable
    return [t.id for t in sorted(wf.get_all_unique_tasks(), key=lambda x: x.id)]

# def load_init_pops(path):
#     with open(path, 'r') as f:
#         data = json.load(f)
#     for el in data["iterations"][0]:
#         if el["gen"] == 0:

class ArchivedSelector:
    def __init__(self, size):
        self._hallOfFame = tools.HallOfFame(size)
        pass

    def __call__(self, selector):
        def wrapper(ctx, pop):
            self._hallOfFame.update(pop)
            new_pop = list(self._hallOfFame)
            offspring = selector(pop, len(pop) - len(new_pop))
            new_pop += offspring
            return new_pop
        return wrapper
    pass