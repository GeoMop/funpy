from typing import Union, Tuple, List, Dict, Any, Callable
from functools import cached_property, wraps

import logging
import os
import sys
import shutil
import pickle
import functools
import hashlib
import cloudpickle
import io
import pickletools

import datetime
from funpy.joblib import Memory
import attrs
import hashlib
from functools import wraps
import time
import os
import pathlib
from funpy.result_cache import FileCache

"""
TODO:
- merge test_memoize and test_memoize_recursion
- test_joblib 
   - use cloudpickle in Memory implementation (see changes in _changes)
   - test and debug persistence (test_joblib.py, caching of A instances are not persistent)
   - add debug functionality to Memory
   
   
- StoreBackendMixin - refactor to only perform write, not pickling, pickling 
  A dumping function should be passed taking a stream a an argument. 
  We want to preserve separation of hashing and pickling and allow direct pickling to the file tor large objects.
  
  do the pickling of arguments only at one place used both in hashing as in serialization 

- joblib function hashing is based on "func.__code__.__hash__()" 
- cleanest approach: make outer wrapper that, creates inner wrapper with additional info and then apply joblib wrapper
  at the first wrapper call

- introduce proper dataclass for configuration to this end with properly defined and documented attributes.
- Cache option using the REDIS, support for distributed cache. 
- Better reporting of failed hashing check.
- Support detection of non pure functions .. there is a lib for that
- Extend Cache API by 'contains(hash)' to support shelving, returning an object reference 
  (collable returnring actual value).
- JobLib possibly provide better numpy support.
- allow ignoring given function parameters
- Explicit object hashing using hasing of the serialized as last resort. 
- Possibly base whole implementation on JobLib Memory.cache() ... seems a bit overengineered
  but reasonably. Suppoerts func_info so 
"""

# memoization configuration
@attrs.define
class MemoizeCfg:
    """
    Usage:
    from funpy import memoize, MemoizeCfg
    MemoizeCfg.instance(debug, cache_dir, ...)
    ...
    @memoize
    def fun(...):
        ...

    """
    debug: bool = False
    # Do not cache the calls, but check if same inputs leads to same outputs. Usefull if you suspect the caching doesn't work properly.
    cache_dir: Union[pathlib.Path, str] = "funpy_cache"
    # Relative path is relative to the home directory. Recommended as absolute paths are not portable.
    # Do not use this directly in implementation use 'cache_path' property instead.

    @cached_property
    def cache_path(self):
        """
        Normalize cache path and create its directory if necessary.
        :return:
        """
        path = pathlib.Path(self.cache_dir)
        if not path.is_absolute():
            path = pathlib.Path.home() / path
        return path

    __instance__ = None
    @classmethod
    def instance(cls, *args, **kwargs):
        if cls.__instance__ is None:
            cls.__instance__ = cls(*args, **kwargs)
        return cls.__instance__

    # @cached_property
    # def memory(self):
    #     return Memory(str(self.cache_path), verbose=0)

    @cached_property
    def cache(self):
        return FileCache(cache_dir=self.cache_path)

    def clear(self):
        self.cache.clear()

    def reduce_size(self):
        self.cache.reduce_size(),


def configure(**kwargs):
    """
    Set memoization options. Global variable.
    """

def memoize(func):
    cloudpickle.register_pickle_by_value(sys.modules[func.__module__])
    cfg = MemoizeCfg.instance()
    cache = cfg.cache

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        m = hashlib.sha256()
        pickled_fn_call = cloudpickle.dumps((func, args, kwargs))
        m.update(pickle_remove_duplicit(pickled_fn_call))
        hash_fn_call = m.digest()

        fun_name = func.__name__
        value = cache.value(hash_fn_call, fun_name)
        if value is cache.NoValue:
            result = func(*args, **kwargs)
            data = {
                "func": func,
                "args": args,
                "kwargs": kwargs,
                "result": result
            }
            cache.insert(hash_fn_call, data, fun_name)
        else:
            result = value["result"]
            if cfg.debug:
                new_result = func(*args, **kwargs)
                if new_result != result:
                    # Detection of faulty hashing.
                    # TODO: better reporting
                    print("Function {}, new result is different from cache result.".format(fun_name))

        return result

    #wrapper.__name__ += "_memoize"
    return wrapper



#
# def memoize(*args, **kwargs):
#     """
#     Decorator to memoize call of a function.
#     The call is parametrized both by the function argumants as the function actual implementation.
#     A hash value is computed from the call arguments and the function 'code' if the hash
#     is found in the global results cache (see. MemoizeConfig.cache) the stored result value is used instead of the function call.
#     Otherwise, the function call is evaluated and return value stored in the cache.
#     This allows using caching during development.
#     Memoization can go wrong if:
#     - functon has side effects, not a PURE FUNCITION
#     - some arguments are wrongly pickled
#
#     Argument hashing depends on 'cloudpickle' which could serialize most of Python objects,
#     but still may have problems in some corner cases. Consult 'cloudpickle' documentation
#     how to customize serialization.
#
#     Code hashing implementation details:
#     The function code is inspected and any other referenced (called) functions are hashed recursively if
#     a) they are from the same module as decorated 'func'
#     b) they are decorated by memoize itself
#
#     :return: decorated function
#     """
#
#     def decorator(func):
#         memoize_cfg = MemoizeCfg.instance()
#         return MemoizedFn(memoize_cfg, func, *args, **kwargs)
#
#     # Check if decorator was called with arguments or directly on the function
#     if len(args) == 1 and callable(args[0]):
#         # Case 1: No arguments provided, called directly on the function
#         func = args[0]
#         return decorator(func)
#     else:
#         # Case 2: Arguments provided, return a decorator
#         return decorator
#
#
# class MemoizedFn:
#     def __init__(self, cfg, func, *args, **kwargs):
#         self.cfg: MemoizeCfg = cfg
#         self.func = func
#         self._fn_hash = None
#         self.memory_cache = lambda func : cfg.memory.cache(func, *args, cache_validation_callback= self.debug_validation, **kwargs)
#         # partial substitution
#
#         # Register pickling of referenced functions from the same module
#         # Their call is not memoized but their code is tracked for changes.
#         cloudpickle.register_pickle_by_value(sys.modules[func.__module__])
#
#     def fn_hash(self):
#         m = hashlib.sha256()
#         pickled_fn = cloudpickle.dumps(self.func)
#         m.update(pickle_remove_duplicit(pickled_fn))
#         hash_fn_call = m.digest()
#         return hash_fn_call
#
#     def __call__(self, *args, **kwargs):
#         if self._fn_hash is None:
#             self._fn_hash = self.fn_hash(self.func)
#
#             @functools.wraps(self.func)
#             def inner_wrapper(*args, __memoized_fn__=self, **kwargs):
#                 return self.func(*args, **kwargs)
#             self._inner_warapper = inner_wrapper
#
#             self._wrapped_fn = self.memory.cache(self._inner_warapper)
#             self._wrapped_fn.__name__ += "_memoize"
#         return self._wrapped_fn(*args, **kwargs)
#
#
#         # Should be joblib.MemorizedFunc instance
#
#     #
#     #     fun_name = func.__name__
#     #     value = result_cache.value(hash_fn_call, fun_name)
#     #     if value is result_cache.NoValue:
#     #         result = func(*args, **kwargs)
#     #         data = {
#     #             "func": func,
#     #             "args": args,
#     #             "kwargs": kwargs,
#     #             "result": result
#     #         }
#     #         result_cache.insert(hash_fn_call, data, fun_name)
#     #     else:
#     #         result = value["result"]
#     #         if _config["debug"]:
#     #             new_result = func(*args, **kwargs)
#     #             if new_result != result:
#     #                 # Detection of faulty hashing.
#     #                 # TODO: better reporting
#     #                 print("Function {}, new result is different from cache result.".format(fun_name))
#     #
#     #
#     #     return result
#     #
#     #
#     #
#     # return wrapper


def pickle_remove_duplicit(pkl):
    """
    Optimize a pickle microcode by removing unused PUT opcodes and duplicated unicode strings.
    This is critical to avoid spurious hash differences.
    """
    put = 'PUT'
    get = 'GET'
    oldids = set()  # set of all PUT ids
    newids = {}     # set of ids used by a GET opcode; later used to map used ids.
    opcodes = []    # (op, idx) or (pos, end_pos)
    proto = 0
    protoheader = b''
    strings = {}
    memo_map = {}
    last_opcode_name = ""
    last_arg = None

    # Generate all opcodes and store positions to calculate end_pos
    ops = list(pickletools.genops(pkl))
    for i, (opcode, arg, pos) in enumerate(ops):
        # Determine end_pos by looking at the position of the next opcode
        end_pos = ops[i + 1][2] if i + 1 < len(ops) else len(pkl)

        if 'PUT' in opcode.name:
            assert opcode.name in ('PUT', 'BINPUT'), f"{opcode.name}"
            oldids.add(arg)
            opcodes.append((put, arg))
        elif opcode.name == 'MEMOIZE':
            idx = len(oldids)

            # Inserted into optimize
            if 'BINUNICODE' in last_opcode_name:
                assert last_opcode_name in ('BINUNICODE', 'SHORT_BINUNICODE'), f"{last_opcode_name}"
                if last_arg in strings:
                    opcodes.pop()
                    strid = strings[last_arg]
                    newids[strid] = None
                    opcodes.append((get, strid))
                    memo_map[idx] = strid
                else:
                    strings[last_arg] = idx

            oldids.add(idx)
            opcodes.append((put, idx))
        elif 'FRAME' in opcode.name:
            assert 'FRAME' == opcode.name, f"{opcode.name}"
            pass
        elif 'GET' in opcode.name:
            assert opcode.name in ('GET', 'BINGET'), f"{opcode.name}"
            if opcode.proto > proto:
                proto = opcode.proto

            # inserted into optimize
            if arg in memo_map:
                arg = memo_map[arg]

            newids[arg] = None
            opcodes.append((get, arg))
        elif opcode.name == 'PROTO':
            if arg > proto:
                proto = arg
            if pos == 0:
                protoheader = pkl[pos:end_pos]
            else:
                opcodes.append((pos, end_pos))
        else:
            opcodes.append((pos, end_pos))
        last_opcode_name = opcode.name
        last_arg = arg
    del oldids

    # Copy the opcodes except for PUTS without a corresponding GET
    out = io.BytesIO()
    # Write the PROTO header before any framing
    out.write(protoheader)
    pickler = pickle._Pickler(out, proto)
    if proto >= 4:
        pickler.framer.start_framing()
    idx = 0
    for op, arg in opcodes:
        frameless = False
        if op is put:
            if arg not in newids:
                continue
            data = pickler.put(idx)
            newids[arg] = idx
            idx += 1
        elif op is get:
            assert newids[arg] is not None
            data = pickler.get(newids[arg])
        else:
            data = pkl[op:arg]
            frameless = len(data) > pickler.framer._FRAME_SIZE_TARGET
        pickler.framer.commit_frame(force=frameless)
        if frameless:
            pickler.framer.file_write(data)
        else:
            pickler.write(data)
    pickler.framer.end_framing()
    return out.getvalue()



class File:
    """
    An object that should represent a file as a computation result.
    Contains the path and the file content hash.
    The system should also prevent modification of the files that are already created.
    To this end one has to use File.open instead of the standard open().
    Current usage:

    with File.open(path, "w") as f:
        f.write...

    return File.from_handle(f)  # check that handel was opened by File.open and is closed, performs hash.

    Ideally, the File class could operate as the file handle and context manager.
    However that means calling system open() and then modify its __exit__ method.
    However I was unable to do that. Seems like __exit__ is changed, but changed to the original one smowere latter as
    it is not called. Other possibility is to wrap standard file handle and use it like:

    @joblib.task
     def make_file(file_path, content):`
        with File.open(file_path, mode="w") as f: # calls self.handle = open(file_path, mode)
            f.handle.write(content)
        # called File.__exit__ which calls close(self.handle) and performs hashing.
        return f

    TODO: there is an (unsuccessful) effort to provide special handle for writting.
    TODO: Override deserialization in order to check that the file is unchanged.
          Seems that caching just returns the object without actuall checking.
    """

    # @classmethod
    # def handle(cls, fhandle):
    #     return File(fhandle.name)

    # @classmethod
    # def output(cls, path):
    #     """
    #     Create File instance intended for write.
    #     The hash is computed after call close of the of open() handle.
    #     Path is checked to not exists yet.
    #     """
    #     return cls(path, postponed=True)
    _hash_fn = hashlib.md5
    def __init__(self, path: str, files:List['File'] = None):  # , hash:Union[bytes, str]=None) #, postponed=False):
        """
        For file 'path' create object containing both path and content hash.
        Optionaly the files referenced by the file 'path' could be passed by `files` argument
        in order to include their hashes.
        :param path: str
        :param files: List of referenced files.
        """
        self.path = os.path.abspath(path)
        if files is None:
            files = []
        self.referenced_files = files
        self._set_hash()

    def __getstate__(self):
        return (self.path, self.referenced_files)

    def __setstate__(self, args):
        self.path, self.referenced_files = args
        self._set_hash()

    def _set_hash(self):
        files = self.referenced_files
        md5 = self.hash_for_file(self.path)
        for f in files:
            md5.update(repr(f).encode())
        self.hash = md5.hexdigest()

    @staticmethod
    def open(path, mode="wt"):
        """
        Mode could only be 'wt' or 'wb', 'x' is added automaticaly.
        """
        exclusive_mode = {"w": "x", "wt": "xt", "wb": "xb"}[mode]
        # if os.path.isfile(path):
        #    raise ""
        fhandle = open(path, mode=exclusive_mode)  # always open for exclusive write
        return fhandle

    @classmethod
    def from_handle(cls, handle):
        assert handle.closed
        assert handle.mode.find("x") != -1
        return cls(handle.name)

    def __hash__(self):
        if self.hash is None:
            raise Exception("Missing hash of output file.")
        return hash(self.path, self.hash)

    def __str__(self):
        return f"File('{self.path}', hash={self.hash})"


    """
    Could be used from Python 3.11    
    @staticmethod
    def hash_for_file(path):
        with open(path, "rb") as f:
            return hashlib.file_digest(f, "md5")

        md5 = hashlib.md5()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                md5.update(chunk)
        return md5.digest()
    """

    @staticmethod
    def hash_for_file(path):
        '''
        Block size directly depends on the block size of your filesystem
        to avoid performances issues
        Here I have blocks of 4096 octets (Default NTFS)
        '''
        block_size = 256 * 128
        md5 = File._hash_fn()
        try:
            with open(path, 'rb') as f:
                for chunk in iter(lambda: f.read(block_size), b''):
                    md5.update(chunk)
        except FileNotFoundError:
            raise FileNotFoundError(f"Missing cached file: {path}")
        return md5


"""


"""
