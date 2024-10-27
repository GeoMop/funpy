from funpy import memoize, report, FileCache, MemoizeCfg
from funpy.memoize import pickle_remove_duplicit
import os
import time
import pytest
import pickle
import pickletools
import logging

script_dir = os.path.dirname(os.path.realpath(__file__))
cache_dir = os.path.join(script_dir, "cache_data")

# Test file result memoization.
# @memoize
# def file_func(f:common.File, out_file:str) -> common.File:
#     print(f"\nfile_func(f={f}, out_file={out_file})")
#     with open(f.path, "r") as ff:
#         content = ff.read()
#
#     f_name = out_file
#     with common.File.open(f_name, "w") as ff:      # need to use File.open that would check that the file does'nt exist
#         ff.write(content)
#         print(f"Appended: {out_file}", file=ff)
#
#     return common.File.from_handle(ff)  # Have to create the File object explicitely after handle is closed.


# def test_file_memoization():
#     cache = common.EndorseCache.instance()
#     cache.expire_all()
#
#     input_file = "sandbox/memoize_file.txt"
#     output_file = "sandbox/output_file.txt"
#     with open(input_file, "w") as ff:
#         ff.write(f"First line.")
#     try:
#         os.remove(output_file)
#     except OSError:
#         pass
#     ####
#     f = common.File(input_file)
#
#     f1 = file_func(f, output_file)    # Create output_file.txt
#     f2 = file_func(f, output_file)    # Re create output_file.txt, skipped.
#     with pytest.raises(FileExistsError):
#         f3 = file_func(f1, output_file)   # Trying to overwrite the created file. Should raise.
#     """
#     Test file pickle and depickle with file validation.
#     """

@report
@memoize
def func(a:int):
    print("\nCompute func.")
    time.sleep(0.1)
    return a * a

def test_memoization_reporting():
    #logging.basicConfig(level=logging.DEBUG)
    f1 = func(2)
    f2 = func(2)
    f3 = func(f1 + f2)




def test_cache():
    cache = FileCache(cache_dir=cache_dir)
    cache.clear()

    hash = b"abc"
    value = (1, "hello")
    fun_name = "name"

    cache.insert(hash, value, fun_name)

    from_cache = cache.value(hash, fun_name)

    assert from_cache == value


@memoize
def func(a:int):
    print("\nCompute func.")
    time.sleep(2)
    return a * a


def test_memoization():
    cache = FileCache(cache_dir=cache_dir)
    cache.clear()

    f1 = func(2)
    f2 = func(2)
    f3 = func(f1 + f2)


@memoize
def fce(a):
    return a



def read_hash():
    dir = MemoizeCfg.instance().cache_path / "fce"
    if not dir.is_dir():
        return None
    return list(dir.iterdir())


# hash for input
def hi(a):
    MemoizeCfg.instance().cache.clear()
    fce(a)
    return read_hash()


def test_inputs():
    # basic
    assert hi(1) == hi(1)
    assert hi(1) != hi(2)

    # custom classes
    class A:
        def __init__(self, a):
            self.a = a

    a = A(1)
    b = A(2)

    assert hi(a) == hi(a)
    assert hi(a) != hi(b)

    # functions
    a = lambda x: 1
    b = lambda x: 2

    assert hi(a) == hi(a)
    assert hi(a) != hi(b)


def test_pickle_remove_duplicit():
    a = "he"
    b = "llo"
    c = a + b
    d = a + b

    bin1 = pickle.dumps((c, c))
    bin2 = pickle.dumps((c, d))
    bin3 = pickle.dumps((c, c, c, c))
    bin4 = pickle.dumps((c, d, c, d))

    assert pickletools.optimize(bin1) == pickle_remove_duplicit(bin2)
    assert pickletools.optimize(bin3) == pickle_remove_duplicit(bin4)
