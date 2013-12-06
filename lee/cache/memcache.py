from lee.conf import memcached
import memcache
__all__ = ['get', 'set', 'delete', 'incr', 'decr']
mc = memcache.Client(memcached)
get = mc.get
set = mc.set
delete = mc.delete
incr = mc.incr
decr = mc.decr
