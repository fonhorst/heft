# Script to be launched with: python -m scoop scriptName.py
import random
import operator
from scoop import futures, logger

data = [random.randint(-1000, 1000) for r in range(1000)]


def add(x, y):
    s = "{0}+{1}".format(x, y)
    logger.warn(s)
    return x + y

if __name__ == '__main__':
    # Python's standard serial function
    serialSum = sum(map(abs, data))



    # SCOOP's parallel function
    parallelSum = futures.mapReduce(abs, add, data)

    assert serialSum == parallelSum
