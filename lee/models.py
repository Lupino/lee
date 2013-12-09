from .utils import unparse

__all__ = ['Model']

class Model(dict):

    table_name = 'default'
    columns = []
    auto_cache = True
    cache_timeout = 0
    auto_create_table = True
    __slots__ = ['_table']

    def __init__(self, table, payload = {}):
        if payload:
            payload = unparse(payload, self.columns)
        dict.__init__(self, payload)
        self._table = table

    def __repr__(self):
        columns = '\n#. '.join([str(col) for col in self.columns])
        return 'Table[{}] columns: \n#. {}'.format(self.table_name, columns)

    def save(self):
        return self._table.save(self.copy())

    def delete(self):
        pris = list(filter(lambda x: x.get('primary'), self.columns))
        args = tuple(map(lambda x: self.get(x['name']), pris))
        return self._table.del_by_id(*args)
