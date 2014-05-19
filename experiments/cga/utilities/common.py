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



