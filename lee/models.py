from . import conf
from .utils import unparse

__all__ = ['Model']

class Model(dict):

    table_name = 'default'
    columns = []
    auto_cache = True
    cache_timeout = 0
    auto_create_table = True

    def __init__(self, table, payload = {}):
        if payload:
            payload = unparse(payload, self.columns)
        dict.__init__(self, payload)
        self._table = table
        self.cache_timeout = self.cache_timeout or conf.cache_timeout

    def save(self):
        return self._table.save(self.copy())

    def delete(self):
        pris = list(filter(lambda x: x.get('primary'), self.columns))
        args = tuple(map(lambda x: self.get(x['name']), pris))
        return self._table.del_by_id(*args)
