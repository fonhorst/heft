from uuid import uuid4
from deap import creator

"""
This file contains only wrappers type for individuals
participating in evolution processes. It was created due to the fact that
pure base types of Python like dict or list cannot be extended with additional fields,
which is useful for transferring information during an evolution process.
(NOTE: of cource we can create and use wrappers class explicitly after entering into an evolution process procedure,
but for the sake of simplicity and referring to deap examples of code, It have been decided use mentioned above scheme)

"""

# creator.create("DictBasedIndividual", dict, uid=lambda: uuid4())
# DictBasedIndividual = creator.DictBasedIndividual

## we cannot create uid property through creator.create due to its internal algorithm
class DictBasedIndividual(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uid = uuid4()
    pass

creator.create("ListBasedIndividual", list)
ListBasedIndividual = creator.ListBasedIndividual
