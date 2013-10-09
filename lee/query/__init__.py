from lee import conf

def _dispatch():
    if conf.use_mysql:
        from . import oursql as query
    else:
        from . import sqlite as query
    return query

class query(object):
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

def create_table(*args, **kwargs):
    return _dispatch().create_table(*args, **kwargs)

def show_tables(*args, **kwargs):
    return _dispatch().show_tables(*args, **kwargs)

def diff_table(*args, **kwargs):
    return _dispatch().diff_table(*args, **kwargs)
