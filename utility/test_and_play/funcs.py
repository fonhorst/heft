from datetime import datetime


def f1():
    print("f1 start: " + str(datetime.now()))
    #[simple_func(i) for i in l[0:5000]]
    for k in range(5000):
        i = k
        for j in range(20000):
            i += j
        # print("f1")
    print("f1 end: " + str(datetime.now()))
    pass

def f2():
    print("f2 start: " + str(datetime.now()))
    # [simple_func(i) for i in l[5000:10000]]
    for k in range(5000):
        i = k
        for j in range(20000):
            i += j
        # print("f2")
    print("f2 end: " + str(datetime.now()))
    pass
