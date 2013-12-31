from lee import conf

__all__ = ['query', 'create_table', 'show_tables', 'diff_table', 'desc_table']

def _dispatch():
    if conf.use_mysql:
        from . import oursql as query
    else:
        from . import sqlite as query
    return query

class query(object):
    __slots__ = ['autocommit', 'keyword']
    def __init__(self, keyword='cur', autocommit=False):
        '''
            @keyword: default is cur
            @autocommit: if need commit set it True. default is False
        '''
        self.autocommit = autocommit
        self.keyword = keyword

    def __call__(self, callback):
        def wrapper(*args, **kwargs):
            return _dispatch().query(self.keyword, self.autocommit)\
                    (callback)(*args, **kwargs)

        return wrapper

def create_table(table_name, columns, spec_index=(), spec_uniq=()):
    return _dispatch().create_table(table_name, columns, spec_index, spec_uniq)

def show_tables():
    return _dispatch().show_tables()

def diff_table(table_name, columns, spec_index=(), spec_uniq=()):
    return _dispatch().diff_table(table_name, columns, spec_index, spec_uniq)

def desc_table(table_name):
    return _dispatch().desc_table(table_name)
