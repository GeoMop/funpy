from typing import Union, Tuple, List, Dict, Any
from functools import cached_property
import pathlib
import attrs
import datetime
import pickle
import cloudpickle
import shutil

@attrs.define
class ResultCacheBase:
    cache_dir: Union[pathlib.Path, str] = "funpy_cache"
    # Relative path is relative to the home directory. Recommended as absolute paths are not portable.
    # Do not use this directly in implementation use 'cache_path' property instead.

    class NoValue:
        """
        NoValue indicates missing item in the cache. Must differentiate from 'None'
        which is valid cache value.
        """
        pass


    @cached_property
    def cache_path(self):
        """
        Normalize cache path and create its directory if necessary.
        :return:
        """
        path = pathlib.Path(self.cache_dir)
        if not path.is_absolute():
            path = pathlib.Path.home() / path
        path.mkdir(parents=True, exist_ok=True)
        return path




class FileCache(ResultCacheBase):
    """
    A simple result cache using new file for each cached call.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    def _hash_file(self, call_hash: bytes, fun_name: str) -> pathlib.Path:
        fun_dir = self.cache_path / fun_name
        fun_dir.mkdir(parents=True, exist_ok=True)
        return fun_dir / call_hash.hex()

    def value(self, call_hash: bytes, fun_name: str = '_anonymous_') -> Any:
        """
        Ask for the stored call of the function 'fun_name'.
        The function with its arguments is represented by its 'hash'.
        Return NoValue class if result not found.
        """
        file_path = self._hash_file(call_hash, fun_name)
        if not file_path.exists():
            return self.NoValue

        with open(file_path, "rb") as f:
            bin_data = f.read()

        value = pickle.loads(bin_data)
        return value


    def insert(self, call_hash: bytes, value: Any, fun_name: str = '_anonymous_'):
        """
        Store the return value of the call to the function 'fun_name'.
        The function with its arguments is represented by its 'hash'.
        """
        bin_data = cloudpickle.dumps(value)
        file_path = self._hash_file(call_hash, fun_name)
        with open(file_path, "wb") as f:
            f.write(bin_data)


    def clear(self):
        """
        Empty the cache.
        Remove just content of the cache dir in order to preserve its permissions and ownership.
        """
        dir_path = self.cache_path
        if dir_path.is_dir():
            for item in dir_path.iterdir():
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()  # Remove file directly
        else:
            raise NotADirectoryError(f"{dir_path} is not a directory")

