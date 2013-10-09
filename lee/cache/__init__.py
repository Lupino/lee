from lee import conf

def _dispatch():
    if conf.memcached:
        from . import memcache as mc
    elif conf.lru_cache:
        from . import lru_cache as mc
    else:
        from . import uncache as mc
    return mc

def get(*args, **kwargs):
    return _dispatch().get(*args, **kwargs)

def set(*args, **kwargs):
    return _dispatch().set(*args, **kwargs)

def delete(*args, **kwargs):
    return _dispatch().delete(*args, **kwargs)

def incr(*args, **kwargs):
    return _dispatch().incr(*args, **kwargs)

def decr(*args, **kwargs):
    return _dispatch().decr(*args, **kwargs)

def gen_key(*args):
    args = map(str, args)
    return ':'.join(args)
