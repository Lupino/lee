from .query import create_table, show_tables, diff_table, query
from .utils import parse, parse_query
from . import cache as mc
from lee.logging import logger
from lee import conf

__all__ = ['Table']

_query = query

class Table(object):
    TABLES = None
    def __init__(self, model):
        self._model = model
        self._pris = []
        self._uniqs = []

        if Table.TABLES is None:
            Table.TABLES = show_tables()

        if model.auto_create_table and model.table_name not in Table.TABLES:
            Table.TABLES.append(model.table_name)
            create_table(model.table_name, model.columns)

        self.defaults = {}
        for column in model.columns:
            if column.get('primary'):
                self._pris.append(column['name'])
            elif column.get('unique'):
                self.gen_uniq_query(column['name'])
                self.gen_uniq_del(column['name'])
                self._uniqs.append(column['name'])

            if column.get('default') is not None:
                self.defaults[column['name']] = column['default']

        self._pri_field = ', '.join(['`{}`'.format(pri) for pri in self._pris])
        self.gen_pris_query()
        self.gen_pris_del()

    def __call__(self, *args, **kwargs):
        return self._model(self, *args, **kwargs)

    def diff_table(self):
        return diff_table(self._model.table_name, self._model.columns)

    def gen_uniq_query(self, column_name):

        @query()
        def _gen_uniq_query(uniq_key, cur):
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

        setattr(self, 'find_by_{}'.format(column_name), _gen_uniq_query)

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
        mc.set(mc_key, obj, self._model.cache_timeout)

    def _cache_del(self, obj):
        if isinstance(obj, (tuple, list)):
            args = obj
        else:
            args = [obj[pri] for pri in self._pris]
        mc_key = self._gen_cache_key(args)
        mc.delete(mc_key)

    def gen_pris_query(self):
        pri_len = len(self._pris)
        @query()
        def _gen_pri_query(*args, cur):
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

        self._find_by_id = _gen_pri_query

    def find_by_id(self, *args):
        return self._find_by_id(*args)

    def gen_uniq_del(self, column_name):
        @query(autocommit=True)
        def _gen_uniq_del(uniq_key, cur):
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
        setattr(self, 'del_by_{}'.format(column_name), _gen_uniq_del)

    def gen_pris_del(self):
        pri_len = len(self._pris)

        @query(autocommit=True)
        def _gen_pri_del(*args, cur):
            if len(args) == pri_len:
                if self._model.auto_cache and conf.is_cache:
                    self._cache_del(args)

                where = ' AND '.join(['`{}` = ?'.format(pri) for pri in self._pris])
                sql = 'DELETE FROM `{}` WHERE {}'.format( \
                        self._model.table_name, where)
                logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))
                cur.execute(sql, args)

        self._del_by_id = _gen_pri_del

    def del_by_id(self, *args):
        self._del_by_id(*args)

    @query(autocommit=True)
    def save(self, obj, cur):
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
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))
            cur.execute(sql, args)
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
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))

            cur.execute(sql, args)

            return cur.lastrowid

    def find_one(self, query = None, column = '*', order = None, group = None,
            is_or = False):

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
