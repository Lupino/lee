from _thread import RLock
from lee import conf
_cache = {}
full = False
key_list = []

lock = RLock()
def get(key, *args, **kwargs):
    global _cache, full, key_list, lock
    with lock:
        if key in key_list:
            key_list.remove(key)
            key_list.insert(0, key)
        return _cache.get(key)

def set(key, val, *args, **kwargs):
    global _cache, full, key_list, lock
    with lock:
        if key in key_list:
            _cache[key] = val
            key_list.remove(key)
            key_list.insert(0, key)
        elif full:
            old_key = key_list.pop()
            key_list.insert(0, key)
            _cache.pop(old_key)
            _cache[key] = val
        else:
            key_list.insert(0, key)
            _cache[key] = val
            full = (len(_cache) >= conf.lru_cache_max)

def delete(key, *args, **kwargs):
    global _cache, full, key_list, lock
    with lock:
        if key in key_list:
            key_list.remove(key)
            _cache.pop(key)
            full = (len(_cache) >= conf.lru_cache_max)

def incr(key, *args, **kwargs):
    global _cache, full, key_list, lock
    with lock:
        val = _cache.get(key, 0)
        val += 1
        if key in key_list:
            _cache[key] = val
            key_list.remove(key)
            key_list.insert(0, key)
        elif full:
            old_key = key_list.pop()
            key_list.insert(0, key)
            _cache.pop(old_key)
            _cache[key] = val
        else:
            key_list.insert(0, key)
            _cache[key] = val
            full = (len(_cache) >= conf.lru_cache_max)

        return _cache.get(key)

def decr(key, *args, **kwargs):
    global _cache, full, key_list, lock
    with lock:
        val = _cache.get(key, 0)
        val -= 1
        if key in key_list:
            _cache[key] = val
            key_list.remove(key)
            key_list.insert(0, key)
        elif full:
            old_key = key_list.pop()
            key_list.insert(0, key)
            _cache.pop(old_key)
            _cache[key] = val
        else:
            key_list.insert(0, key)
            _cache[key] = val
            full = (len(_cache) >= conf.lru_cache_max)

        return _cache.get(key)
