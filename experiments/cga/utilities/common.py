import json
import os
import uuid

## TODO: make a blocking wrapper
from scoop import futures


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
    # fs = [futures.submit(func) for i in range(n)]
    # futures.wait(fs)
    # return [f.result() for f in fs]
    return [func() for i in range(n)]