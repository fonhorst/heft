import json
import os
import uuid

## TODO: make a blocking wrapper
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
        pass


def load_data(path):
    with open(path, "r") as f:
        result = json.load(f)
    return result


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

