from .utils import unparse

__all__ = ['Model']

class Model(object):

    table_name = 'default'
    columns = []
    auto_cache = True
    cache_timeout = 0
    auto_create_table = True
    spec_index = ()
    spec_uniq = ()
    __slots__ = ['_table', '__dict__', '_changed']

    def __init__(self, table, payload = {}):
        if payload:
            payload = unparse(payload, self.columns)

        self._table = table
        self._changed = {}
        self.update(payload)

    def __repr__(self):
        columns = '\n#. '.join([str(col) for col in self.columns])
        return 'Table[{}] columns: \n#. {}'.format(self.table_name, columns)

    def __getitem__(self, key, default=None):
        '''x.__getitem__(y) <==> x[y]'''
        return self.__dict__.get(key, default)

    def __setitem__(self, key, val):
        '''x.__setitem__(i, y) <==> x[i]=y'''
        if isinstance(val, str):
            val = val.strip()
        self.__dict__[key] = val
        self._changed[key] = val

    def keys(self):
        '''D.keys() -> a set-like object providing a view on D's keys'''
        return self.__dict__.keys()

    def values(self):
        '''D.values() -> an object providing a view on D's values'''
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def pop(self, key, default=None):
        '''
        D.pop(k[,d]) -> v, remove specified key and return the corresponding value.
        If key is not found, d is returned if given, otherwise KeyError is raised
        '''
        return self.__dict__.pop(key, default)

    def get(self, key, default=None):
        '''D.get(k[,d]) -> D[k] if k in D, else d.  d defaults to None.'''
        return self.__dict__.get(key, default)

    def update(self, item):
        '''
        D.update([E, ]**F) -> None.
        * Update D from dict/iterable E and F.
        * If E present and has a .keys() method, does:     for k in E: D[k] = E[k]
        * If E present and lacks .keys() method, does:     for (k, v) in E: D[k] = v
        * In either case, this is followed by: for k in F: D[k] = F[k]
        '''
        for k, v in item.items():
            if isinstance(v, str):
                item[k] = v.strip()
        return self.__dict__.update(item)

    def copy(self):
        return self.__dict__.copy()

    def save(self):
        return self._table.save(self.__dict__.copy())

    def strict_save(self):
        return self._table.strict_save(self.__dict__.copy(), self._changed.copy())

    def delete(self):
        pris = list(filter(lambda x: x.get('primary'), self.columns))
        args = tuple(map(lambda x: self.get(x['name']), pris))
        return self._table.del_by_id(*args)
