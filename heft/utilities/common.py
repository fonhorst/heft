import inspect
def trace(func):
    def wrap(*args, **kwargs):
        print("Enter to {0}".format(func.__self__))
        res = func(*args, **kwargs)
        print("Exit from {0}".format(func.__self__))
        return res
    return wrap

