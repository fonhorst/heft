import platform
from scoop import futures
data = range(100)

def pr(par):
    for i in range(1000000):
        j = i*34
        pass
    print('%s %s' % (platform.node(), par))
    return par

if __name__ == '__main__':
    # SCOOP's parallel function
    print("Run on: " + platform.node())
    dataParallel = list(futures.map(pr, data))