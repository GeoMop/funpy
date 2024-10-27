import funpy
import pathlib

script_dir = pathlib.Path(__file__).parent
cache_dir = script_dir / "call_cache"

def test_file_cache():
    cache = funpy.FileCache(cache_dir=cache_dir)
    cache.clear()

    # no value
    out = cache.value(b"123", "name")
    assert out is cache.NoValue

    # store with fun name
    hash1 = b"abc"
    value1 = (1, "hello")
    fun_name = "name"
    cache.insert(hash1, value1, fun_name)

    from_cache = cache.value(hash1, fun_name)
    assert from_cache == value1

    # store without fun name
    hash2 = b"ab"
    value2 = (2, "hello")
    cache.insert(hash2, value2)

    from_cache = cache.value(hash2)
    assert from_cache == value2
