from __future__ import print_function
from scoop import futures
from environment.Utility import profile_decorator


## 283.623 for regular map
##  85.314 for scoop.futures.map

def helloWorld(value):
    k = 0
    for i in range(100000):
        for j in range(1000):
            k = i / (j + 1)
            pass
        pass
    return "Hello World from Future #{0}".format(value)

@profile_decorator
def main():
    # returnValues = list(map(helloWorld, range(16)))
    shared_obj = 35
    arr = list(shared_obj for i in range(16))
    returnValues = list(futures.map(helloWorld, arr))
    print("\n".join(returnValues))

if __name__ == "__main__":
    main()

