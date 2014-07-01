import cProfile
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import io
import pstats
import math
from threading import Thread
# from scoop import futures
from utilities.test_and_play.funcs import f1, f2


def simple_func(i):
    for j in range(15000):
        i += j
    # print("h")
    return i

def main_func():


    # execution time: about 28 seconds
    # f1()
    # f2()

    # execution time: about 16 seconds
    with ProcessPoolExecutor() as executor:
        f1_futures = executor.submit(f1)
        f2_futures = executor.submit(f2)

        f1_res = f1_futures.result()
        f2_res = f2_futures.result()

    # execution time: about 28 seconds
    # thread1 = Thread(target=f1)
    # thread1.start()
    # thread2 = Thread(target=f2)
    # thread2.start()
    # thread1.join()
    # thread2.join()


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
