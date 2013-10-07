from .conf import cache_timeout

class Model(dict):

    table_name = 'default'
    columns = []
    auto_cache = True
    cache_timeout = cache_timeout
    auto_create_table = True

    def __init__(self, table, payload = {}):
        dict.__init__(self, payload)
        self._table = table

    def save(self):
        return self._table.save(self)

    def delete(self):
        pris = list(filter(lambda x: x.get('primary'), self.columns))
        args = tuple(map(lambda x: self.get(x['name']), pris))
        return self._table.del_by_id(*args)
