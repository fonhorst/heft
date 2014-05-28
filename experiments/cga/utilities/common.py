from copy import deepcopy
import json
import os
import uuid

## TODO: make a blocking wrapper
from deap import tools
import distance
import math
from scoop import futures
from GA.DEAPGA.coevolution.cga import ListBasedIndividual, rounddeciter
from GA.DEAPGA.coevolution.operators import MAPPING_SPECIE, ORDERING_SPECIE


class UniqueNameSaver:
    def __init__(self, directory):
        self.directory = directory

    def __call__(self, data):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        name = "{0}.json".format(uuid.uuid4())
        path = os.path.join(self.directory, name)
        with open(path, "w") as f:
            json.dump(data, f)
        return name


def load_data(path):
    with open(path, "r") as f:
        result = json.load(f)
    return result

def generate_pathes(folder):
    pathes = []
    for entry in os.listdir(folder):
        p = os.path.join(folder, entry)
        # p = folder + ("" if folder.endswith('/') else "/") + entry
        if os.path.isdir(p):
            pths = generate_pathes(p)
            pathes.extend(pths)
        elif entry.endswith(".json"):
            pathes.append(p)
    return pathes


class ComparableMixin(object):
    def _compare(self, other, method):
        try:
            return method(self._cmpkey(), other._cmpkey())
        except (AttributeError, TypeError):
            # _cmpkey not implemented, or return different type,
            # so I can't compare with "other".
            return NotImplemented

    def __lt__(self, other):
        return self._compare(other, lambda s, o: s < o)

    def __le__(self, other):
        return self._compare(other, lambda s, o: s <= o)

    def __eq__(self, other):
        return self._compare(other, lambda s, o: s == o)

    def __ge__(self, other):
        return self._compare(other, lambda s, o: s >= o)

    def __gt__(self, other):
        return self._compare(other, lambda s, o: s > o)

    def __ne__(self, other):
        return self._compare(other, lambda s, o: s != o)


def repeat(func, n):
    fs = [futures.submit(func) for i in range(n)]
    futures.wait(fs)
    return [f.result() for f in fs]
    # return [func() for i in range(n)]


class OnlyUniqueMutant:
    def __init__(self):
        self.found = set()

    def __call__(self, func):
        def wrapper(ctx, mutant):
            for i in range(50):
                m = deepcopy(mutant)
                func(ctx, m)
                identity = hash(tuple(m))
                if identity not in self.found:
                    for i in range(len(mutant)):
                        mutant[i] = m[i]
                    self.found.add(identity)
                    break
        return wrapper


def extract_initial_pops(path):
    with open(path, "r") as f:
        data = json.load(f)
    initial_pops = data["initial_pops"]

    mps = [ListBasedIndividual([tuple(g) for g in ind]) for ind in initial_pops[MAPPING_SPECIE]]
    os = [ListBasedIndividual(ind) for ind in initial_pops[ORDERING_SPECIE]]
    return mps, os

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
            # offspring = selector(pop, len(pop) - len(new_pop))
            offspring = selector(ctx, pop)
            new_pop += offspring[0:len(pop) - len(new_pop)]
            return new_pop
        return wrapper
    pass

@rounddeciter
def hamming_distances(pop, ideal_ind):
    values = [distance.hamming(ideal_ind, p, normalized=True) for p in pop]
    #mn, mx = min(values), max(values)
    #values = [(v - mn)/mx for v in values]
    return values


def to_seq(mapping):
    srted = sorted(mapping, key=lambda x: x[0])
    return [n for t, n in srted]




# ms_ideal_ind = build_ms_ideal_ind(_wf, rm)
# os_ideal_ind = build_os_ideal_ind(_wf)


def hamming_for_best_components(sols, ms_ideal_ind, os_ideal_ind):
    result = {s_name: hamming_distances([ind], ms_ideal_ind if s_name == MAPPING_SPECIE else os_ideal_ind)[0]
              for s_name, ind in max(sols, key=lambda x: x.fitness).items()}
    return result

def unique_individuals(pop):
    unique_hashes = set(hash(tuple(p)) for p in pop)
    return len(unique_hashes)

def best_components_itself(sols):
    result = {s_name: deepcopy(ind)
              for s_name, ind in max(sols, key=lambda x: x.fitness).items()}
    return result

## measure of phenotype convergence
## as it is normalized, pdm = 1 - pcm
## pdm == phenotype diversity measure
## it means pcm == 1 - we are in a local minimum
def pcm(pop):
    mx = max(pop, key=lambda x: x.fitness).fitness
    mn = min(pop, key=lambda x: x.fitness).fitness

    ## loglp(x) = ln(1 + x)
    sm = lambda arr: sum(math.log1p(math.fabs(p1 - p2)) for p1, p2 in zip(arr[1:len(arr) - 1], arr[2:]))
    numerator = sm(sorted([p.fitness for p in pop]))

    if mx != mn:
        uniform_dist = math.fabs(mx - mn)/(len(pop) - 1)

        i = mn
        arr = []
        while i <= mx:
            arr.append(i)
            i += uniform_dist

        vmd = sm(arr)
        measure = 1 - numerator/vmd
    else:
        measure = 1
    return measure

## genotype diversity measure
## 0 - divesity absence,
## 1 - great diversity
def gdm(pop):
    s1 = 0
    for i in range(len(pop) - 1):
        s2 = 0
        for j in range(i + 1, len(pop)):
            s2 += distance.hamming(pop[i], pop[j], normalized=True)
        s1 += (s2/(len(pop) - i))
    measure = s1 / len(pop)
    return measure


def extract_ordering_from_file(path, wf, estimator, rm):
    with open(path, 'r') as f:
        data = json.load(f)
    solution = data["final_solution"]
    ordering = solution[ORDERING_SPECIE]
    return ordering

def extract_mapping_from_ga_file(path, rm):
    with open(path, 'r') as f:
        data = json.load(f)
    ## TODO: this is pure hack. It is needed to be refactored or removed
    nodes = {node.flops: node.name for node in rm.get_nodes()}

    mapping = ListBasedIndividual([(t, nodes[n]) for t, n in data["mapping"]])
    return mapping

def extract_ordering_from_ga_file(path):
    with open(path, 'r') as f:
        data = json.load(f)

    ordering = ListBasedIndividual([t for t in data["ordering"]])
    return ordering

tourn = lambda ctx, pop: tools.selTournament(pop, len(pop), 2)
## TODO: remove this hack later
class Fitness(ComparableMixin):
    def __init__(self, fitness):
        self.values = [fitness]

    def _cmpkey(self):
        return self.values[0]


## TODO: remake this stub later
def roulette(ctx, pop):

    for p in pop:
        p.fitness = Fitness((1/-1*p.fitness)*100)

    result = tools.selRoulette(pop, len(pop))

    for p in pop:
        p.fitness = (1/(p.fitness.values[0]/100)*-1)
    return result
