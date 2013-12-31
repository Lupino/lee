from .query import create_table, show_tables, diff_table, query
from .utils import parse, parse_query
from . import cache as mc
from .utils import logger
from . import conf
import inspect

__all__ = ['Table']

_query = query

class Table(object):
    '''
    Table.TABLES:
        all the table on the connection database

    Table.defaults:
        the default values of columns

    Table.name:
        the table name

    example::

        class Cache(Model):
            columns = [
                {'name': 'cache_id',   'type': 'str', 'primary': True, 'length': 32},
                {'name': 'value',      'type': 'pickle'},
                {'name': 'created_at', 'type': 'int', 'unsigned': True, 'default': lambda : int(time())}
            ]
            table_name = 'cache'

        Cache = Table(Cache)
        cache = Cache({'cache_id': 'test', 'value': {'test': 'this is a test'}})
        cache.save()
        cache = Cache({'cache_id': 'test1', 'value': {'test': 'this is a test1'}})
        cache.save()
        caches = Cache.find_all()
        print(caches)
        print(cache.find_by_id('test'))
        print(cache.del_by_id('test'))
    '''
    TABLES = None

    __slots__ = ['name', '_model', '_pris', '_uniqs', 'defaults', '_pri_field', '_extra']

    def __init__(self, model):
        self._model = model
        self._pris = []
        self._uniqs = []
        self._extra = {}
        self.name = model.table_name

        if Table.TABLES is None:
            Table.TABLES = show_tables()

        if model.auto_create_table and model.table_name not in Table.TABLES:
            Table.TABLES.append(model.table_name)
            create_table(model.table_name, model.columns, model.spec_index, model.spec_uniq)

        self.defaults = {}
        for column in model.columns:
            if column.get('primary'):
                self._pris.append(column['name'])
            elif column.get('unique'):
                self._uniqs.append(column['name'])

            if column.get('default') is not None:
                self.defaults[column['name']] = column['default']

        self._pri_field = ', '.join(['`{}`'.format(pri) for pri in self._pris])

    def __call__(self, *args, **kwargs):
        return self._model(self, *args, **kwargs)

    def __repr__(self):
        columns = '\n#. '.join([str(col) for col in self._model.columns])
        return 'Table[{}] columns: {}'.format(self._model.table_name, columns)

    def register(self, key, val):
        '''
        register an extra attribute or function to table.

        eg::

            table.register('get', table.find_by_id)
            table.get(primary_key)

        '''
        self._extra[key] = val

    def unregister(self, key):
        '''
        unregister an extra attribute or function to table.

        eg::

            table.register('get', table.find_by_id)
            table.get(primary_key)
            table.unregister('get')
            table.get    # raise

        '''
        return self._extra.pop(key)

    def __getattr__(self, key):
        '''
        get the magic key:
            * Table.del_by_*
            * Table.find_by_*
            * Model.table_*(classmethod)
            * Table._extra[key]

        or raise KeyError
        '''
        column_name = None
        if key.startswith('del_by_'):
            column_name = key[7:]
            if column_name in self._uniqs:
                return self.del_by_uniq(column_name)
        elif key.startswith('find_by_'):
            column_name = key[8:]
            if column_name in self._uniqs:
                return self.find_by_uniq(column_name)

        if key in self._extra.keys():
            return self._extra[key]

        func = getattr(self._model, 'table_{}'.format(key))
        if inspect.ismethod(func):
            return func(self)

        raise KeyError('{} not found'.format(key))

    def diff_table(self):
        '''
        compare the model table and the database table, and show the recommand
        ALERT TABLE SQL
        '''
        return diff_table(self._model.table_name, self._model.columns, self._model.spec_index, self._model.spec_uniq)

    def find_by_uniq(self, column_name, uniq_key=None):
        '''find by uniq key difine on the model column'''
        @query()
        def _find_by_uniq(uniq_key, cur):
            if self._model.auto_cache and conf.is_cache:
                field = self._pri_field
            else:
                field = '*'
            sql = 'SELECT {} FROM `{}` WHERE `{}` = ?'.format(field,
                    self._model.table_name, column_name)
            args = (uniq_key, )
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))
            cur.execute(sql, args)
            ret = cur.fetchone()
            if ret:
                if self._model.auto_cache and conf.is_cache:
                    args = [ret[pri] for pri in self._pris]
                    return self.find_by_id(*args)
                else:
                    return self._model(self, ret)
            return None

        if uniq_key:
            return _find_by_uniq(uniq_key)
        else:
            return _find_by_uniq

    def _gen_cache_key(self, args):
        cols = []
        for k, v in zip(self._pris, args):
            cols.append(k)
            cols.append(v)
        mc_key = mc.gen_key(self._model.table_name, *cols)
        return mc_key

    def _cache_get(self, args):
        mc_key = self._gen_cache_key(args)
        return mc.get(mc_key)

    def _cache_set(self, obj):
        obj = obj.copy()
        args = [obj[pri] for pri in self._pris]
        mc_key = self._gen_cache_key(args)

        if self._model.cache_timeout > 0:
            cache_timeout = self._model.cache_timeout
        else:
            cache_timeout = conf.cache_timeout

        mc.set(mc_key, obj, cache_timeout)

    def _cache_del(self, obj):
        if isinstance(obj, (tuple, list)):
            args = obj
        else:
            args = [obj[pri] for pri in self._pris]
        mc_key = self._gen_cache_key(args)
        mc.delete(mc_key)

    def find_by_id(self, *args):
        '''find by primary key difine on the model column'''
        pri_len = len(self._pris)
        @query()
        def _find_by_id(*args, cur):
            if len(args) == pri_len:
                if self._model.auto_cache and conf.is_cache:
                    ret = self._cache_get(args)
                    if ret:
                        return self._model(self, ret)

                where = ' AND '.join(['`{}` = ?'.format(pri) for pri in self._pris])
                sql = 'SELECT * FROM `{}` WHERE {}'.format( \
                        self._model.table_name, where)
                logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))
                cur.execute(sql, args)
                ret = cur.fetchone()
                if ret:
                    if self._model.auto_cache and conf.is_cache:
                        self._cache_set(ret)
                    return self._model(self, ret)

            return None

        return _find_by_id(*args)

    def del_by_uniq(self, column_name, uniq_key=None):
        '''del by uniq key difine on the model column'''
        @query(autocommit=True)
        def _del_by_uniq(uniq_key, cur):
            if self._model.auto_cache and conf.is_cache:
                sql = 'SELECT {} FROM `{}` WHERE `{}` = ?'.format(self._pri_field,
                        self._model.table_name, column_name)
                args = (uniq_key, )
                logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))
                cur.execute(sql, args)
                ret = cur.fetchone()
                if ret:
                    self._cache_del(ret)
                else:
                    return
            sql = 'DELETE FROM `{}` WHERE `{}` = ?'.format(\
                    self._model.table_name, column_name)
            args = (uniq_key, )
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))
            cur.execute(sql, args)

        if uniq_key:
            return _del_by_uniq(uniq_key)
        else:
            return _del_by_uniq

    def del_by_id(self, *args):
        '''del by primary key difine on the model column'''
        pri_len = len(self._pris)
        @query(autocommit=True)
        def _del_by_id(*args, cur):
            if len(args) == pri_len:
                if self._model.auto_cache and conf.is_cache:
                    self._cache_del(args)

                where = ' AND '.join(['`{}` = ?'.format(pri) for pri in self._pris])
                sql = 'DELETE FROM `{}` WHERE {}'.format( \
                        self._model.table_name, where)
                logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))
                cur.execute(sql, args)

        _del_by_id(*args)

    def save(self, obj):
        '''
        save the obj to database, if has one update it, otherwise create it,
        dect by primary key or unique key
        '''

        @query(autocommit=True)
        def _save(sql, args, cur):
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))
            cur.execute(sql, args)
            return cur.lastrowid

        obj = parse(obj, self._model.columns)

        pris = [obj[pri] for pri in self._pris if pri in obj]
        uniqs = [(uniq, obj[uniq]) for uniq in self._uniqs if uniq in obj]

        use_keys = []
        use_values = []
        for column in self._model.columns:
            if not column.get('primary'):
                column_name = column['name']
                column_value = obj.get(column_name)
                if column_value is not None:
                    use_keys.append(column_name)
                    use_values.append(column_value)
        old_obj = None
        if pris:
            old_obj = self.find_one(list(zip(self._pris, pris)),
                    self._pri_field)
        else:
            for column_name, column_value in uniqs:
                old_obj = self.find_one([(column_name, column_value)],
                        self._pri_field)
                if old_obj:
                    break

        if old_obj:
            part = ', '.join(['`{}`= ?'.format(k) for k in use_keys])
            where = ' AND '.join(['`{}` = ?'.format(pri) for pri in self._pris])
            where = ' WHERE ' + where
            for pri in self._pris:
                use_values.append(old_obj[pri])
            if len(use_values) < 2:
                logger.error('UPDATE {}'.format(str(obj)))
                return None

            sql = 'UPDATE `{}` SET {} {}'.format(self._model.table_name, part, where)
            args = tuple(use_values)
            _save(sql, args)
            if self._model.auto_cache and conf.is_cache:
                self._cache_del(old_obj)

            return None
        else:
            if pris:
                for column_name, column_value in zip(self._pris, pris):
                    use_keys.append(column_name)
                    use_values.append(column_value)

            for key, val in self.defaults.items():
                if key not in use_keys:
                    use_keys.append(key)
                    if callable(val):
                        val = val()
                    use_values.append(val)

            part_k = ', '.join(['`{}`'.format(k) for k in use_keys])
            part_v = ', '.join(['?' for k in use_keys])

            sql = 'INSERT INTO `{}` ({}) VALUES ({})'.format(self._model.table_name, part_k, part_v)
            args = tuple(use_values)

            return _save(sql, args)

    def find_one(self, query = None, column = '*', order = None, group = None,
            is_or = False):

        '''find one by query, also see lee.utils.parse_query'''

        where, values = parse_query(self._model.columns, query, 1, order, group, is_or)

        @_query()
        def _find_one(cur):

            sql = 'SELECT {} FROM `{}` {}'.format(column, self._model.table_name, where)
            args = tuple(values)
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))

            cur.execute(sql, args)

            return cur.fetchone()

        ret = _find_one()
        if ret:
            ret = self._model(self, ret)
        return ret

    def find_all(self, query = None, column = '*', limit = '', order = None,
            group = None, is_or = False, page = None):

        '''find all by query, also see lee.utils.parse_query'''

        if limit and page:
            start = int(limit) * int(page)
            limit = '{}, {}'.format(start, limit)

        where, values = parse_query(self._model.columns, query, limit, order, group,
                is_or)

        @_query()
        def _find_all(cur):

            sql = 'SELECT {} FROM `{}` {}'.format(column, self._model.table_name, where)
            args = tuple(values)
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))

            cur.execute(sql, args)

            return cur.fetchall()

        return [self._model(self, ret) for ret in _find_all()]

    def del_all(self, query = None, limit = '', order = None, group = None,
            is_or = False):

        '''delete all by query, also see lee.utils.parse_query'''

        if self._model.auto_cache and conf.is_cache:
            old_objs = self.find_all(query, self._pri_field, limit, order, group, is_or)

            for old_obj in old_objs:
                self._cache_del(old_obj)

        where, values = parse_query(self._model.columns, query, limit, order, group,
                is_or)

        @_query(autocommit=True)
        def _del_all(cur):
            sql = 'DELETE FROM `{}` {}'.format(self._model.table_name, where)
            args = tuple(values)
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))

            cur.execute(sql, args)

        return _del_all()
