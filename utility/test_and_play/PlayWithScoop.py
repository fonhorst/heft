import cProfile
import io
import pstats
import math
from scoop import futures


def simple_func(i):
    for j in range(1000):
        i += j
    # print("h")
    return i

def main_func():
    l = [i for i in range(100000)]
    # fitnesses = list(map(simple_func, l))
    fitnesses = list(futures.map_as_completed(simple_func, [i for i in range(10000)]))
    pass

if __name__ == '__main__':

    pr = cProfile.Profile()
    pr.enable()

    main_func()

    pr.disable()
    s = io.StringIO()
    sortby = 'cumulative'
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())
