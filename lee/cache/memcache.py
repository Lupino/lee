from lee.conf import memcached
import memcache
mc = memcache.Client(memcached)
get = mc.get
set = mc.set
delete = mc.delete
incr = mc.incr
decr = mc.decr
