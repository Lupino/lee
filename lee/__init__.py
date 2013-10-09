from . import conf
import urllib.parse
import os

def connect(path, memcached=False, cache_timeout=0,
        lru_cache=False, lru_cache_max=128):
    '''connect to the database
    @path:
        sqlite://path/to/the/sqlite
        mysql://host:port?user=dbuser&passwd=dbpasswd&db=dbname
    @memcached: the host list of memcached list
    @cache_timeout: only for memcached timeout
    @lru_cache: bool if use lru_cache set it True
    @lru_cache_max: the max size of lru_cache
    '''
    p = urllib.parse.urlparse(path)
    if p.scheme == 'mysql':
        conf.mysql = {}
        netloc = p.netloc.split(':', 1)
        conf.mysql['host'] = netloc[0]

        if len(netloc) == 2:
            conf.mysql['port'] = int(netloc[1])
        else:
            conf.mysql['port'] = 3306

        conf.mysql.update(urllib.parse.parse_qsl(p.query))

        conf.use_mysql = True
    else:
        conf.path = p.netloc + p.path
        conf.use_mysql = False
        base_path = os.path.dirname(conf.path)
        if not os.path.exists(base_path):
            os.makedirs(base_path)

    conf.memcached = memcached
    conf.lru_cache = lru_cache
    conf.lru_cache_max = lru_cache_max
    conf.cache_timeout = cache_timeout

from .table import Table
from .models import Model
from .query import query
