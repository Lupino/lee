from lee import conf

if conf.memcached:
    from . import memcache as mc
elif conf.lru_cache:
    from . import lru_cache as mc
else:
    from . import uncache as mc

get = mc.get
set = mc.set
delete = mc.delete
incr = mc.incr
decr = mc.decr

def gen_key(*args):
    args = map(str, args)
    return ':'.join(args)
