from copy import deepcopy
import json
import os
import uuid

## TODO: make a blocking wrapper
from scoop import futures
from GA.DEAPGA.coevolution.cga import ListBasedIndividual
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