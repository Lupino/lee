from .utils import unparse

__all__ = ['Model']

class Model(object):

    table_name = 'default'
    columns = []
    auto_cache = True
    cache_timeout = 0
    auto_create_table = True
    __slots__ = ['_table', '_payload']

    def __init__(self, table, payload = {}):
        if payload:
            payload = unparse(payload, self.columns)

        self._table = table
        self._payload = payload

    def __repr__(self):
        columns = '\n#. '.join([str(col) for col in self.columns])
        return 'Table[{}] columns: \n#. {}'.format(self.table_name, columns)

    def __getitem__(self, key, default=None):
        '''x.__getitem__(y) <==> x[y]'''
        return self._payload.get(key, default)

    def __setitem__(self, key, val):
        '''x.__setitem__(i, y) <==> x[i]=y'''
        if isinstance(val, str):
            val = val.strip()
        self._payload[key] = val

    __getattr__ = __getitem__

    def copy(self):
        return self._payload.copy()

    def save(self):
        return self._table.save(self._payload.copy())

    def delete(self):
        pris = list(filter(lambda x: x.get('primary'), self.columns))
        args = tuple(map(lambda x: self.get(x['name']), pris))
        return self._table.del_by_id(*args)
