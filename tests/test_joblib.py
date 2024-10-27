import os
import time
from funpy.joblib import Memory


script_dir = os.path.dirname(os.path.realpath(__file__))
cache_dir = os.path.join(script_dir, "cache_data")

memory = Memory(cache_dir, verbose=0)


@memory.cache
def fce(a):
    time.sleep(0.1)
    return a


# from cache
def is_fce_called(a):

    t = time.time()
    fce(a)
    return time.time() - t > 0.05


def test_joblib_inputs():
    """

    :return:
    """
    # basic
    memory.clear()
    assert is_fce_called(1)
    assert not is_fce_called(1)
    assert is_fce_called(2)

    # custom classes -
    # Following are only enabled by cloudpickle
    class A:
        def __init__(self, a):
            self.a = a


    a = A(1)
    b = A(2)

    #memory.clear()
    # assert is_fce_called(a)
    # assert not is_fce_called(a)
    # assert is_fce_called(b)

    # functions
    a = lambda x: 1
    b = lambda x: 2

    #memory.clear()
    # assert is_fce_called(a)
    # assert not is_fce_called(a)
    # assert is_fce_called(b)
