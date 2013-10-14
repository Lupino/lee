from .query import create_table, show_tables, diff_table, query
from .utils import unparse, parse, parse_query
from . import cache as mc
from lee.logging import logger
from lee import conf

_query = query

class Table(object):
    TABLES = None
    def __init__(self, model):
        self._model = model

        if Table.TABLES is None:
            Table.TABLES = show_tables()

        if model.auto_create_table and model.table_name not in Table.TABLES:
            Table.TABLES.append(model.table_name)
            create_table(model.table_name, model.columns)

        primarys = []
        self.defaults = {}
        for column in model.columns:
            if column.get('primary'):
                primarys.append(column)
            elif column.get('unique'):
                self.gen_query(column)
                self.gen_del(column)

            if column.get('default') is not None:
                self.defaults[column['name']] = column['default']

        self.gen_primary_query(primarys)
        self.gen_primary_del(primarys)

    def __call__(self, *args, **kwargs):
        return self._model(self, *args, **kwargs)

    def diff_table(self):
        return diff_table(self._model.table_name, self._model.columns)

    def gen_query(self, column):

        @query()
        def _gen_query(uniq_key, cur):
            if self._model.auto_cache and conf.is_cache:
                mc_key = mc.gen_key(self._model.table_name, column['name'], uniq_key)
                ret = mc.get(mc_key)
                if ret:
                    ret = unparse(ret, self._model.columns)
                    return self._model(self, ret)

            sql = 'SELECT * FROM `{}` WHERE `{}` = ?'.format(self._model.table_name, column['name'])
            args = (uniq_key, )
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))

            cur.execute(sql, args)

            ret = cur.fetchone()
            if ret:
                ret = unparse(ret, self._model.columns)
                if self._model.auto_cache and conf.is_cache:
                    self._cache_set(ret)
                return self._model(self, ret)
            return None

        if column.get('primary'):
            self._find_by_id = _gen_query
        else:
            setattr(self, 'find_by_{}'.format(column['name']), _gen_query)

    def gen_primary_query(self, primarys):
        pri_len = len(primarys)
        if pri_len == 1:
            return self.gen_query(primarys[0])
        keys = list(map(lambda x: x['name'], primarys))
        def gen(*args):
            if len(args) == pri_len:
                if self._model.auto_cache and conf.is_cache:
                    cols = []
                    for k, v in zip(keys, args):
                        cols.append(k)
                        cols.append(v)
                    mc_key = mc.gen_key(self._model.table_name, *cols)
                    ret = mc.get(mc_key)
                    if ret:
                        return self._model(self, ret)
                ret = self.find_one(list(zip(keys, args)))
                if self._model.auto_cache and conf.is_cache:
                    self._cache_set(ret)
                return ret
            return None

        self._find_by_id = gen

    def find_by_id(self, *args):
        return self._find_by_id(*args)

    def gen_del(self, column):

        @query(autocommit=True)
        def _gen_del(uniq_key, cur):
            if self._model.auto_cache and conf.is_cache:
                mc_key = mc.gen_key(self._model.table_name, column['name'], uniq_key)
                obj = mc.get(mc_key)

                if obj:
                    del_keys = []
                    for col in self._model.columns:
                        if col.get('unique') or col.get('primary'):
                            if obj.get(col['name']):
                                del_keys.append((col['name'], obj.get(col['name'])))

                    for col_name, col_value in del_keys:
                        mc_key = mc.gen_key(self._model.table_name, col_name, col_value)
                        mc.delete(mc_key)

            sql = 'DELETE FROM `{}` WHERE `{}` = ?'.format(self._model.table_name, column['name'])
            args = (uniq_key, )
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))

            cur.execute(sql, args)

        if column.get('primary'):
            self._del_by_id = _gen_del
        else:
            setattr(self, 'del_by_{}'.format(column['name']), _gen_del)

    def gen_primary_del(self, primarys):
        pri_len = len(primarys)
        if pri_len == 1:
            return self.gen_del(primarys[0])
        keys = list(map(lambda x: x['name'], primarys))
        def gen(*args):
            if len(args) == pri_len:
                return self.del_all(list(zip(keys, args)))
            return None

        self._del_by_id = gen

    def del_by_id(self, *args):
        self._del_by_id(*args)

    @query(autocommit=True)
    def save(self, obj, cur):
        obj = parse(obj, self._model.columns)

        primarys = []

        uniqs = []
        use_keys = []
        use_values = []

        for column in self._model.columns:
            key = column['name']
            val = obj.get(key)
            if column.get('primary'):
                primarys.append([key, val])
            else:
                if val is not None:
                    if column.get('unique'):
                        uniqs.append([key, val])
                    else:
                        use_keys.append(key)
                        use_values.append(val)

        old_obj = None
        if primarys and list(filter(lambda x: x[1] is not None, primarys)):
            old_obj = self.find_by_id(*list(map(lambda x: x[1], primarys)))
            for column_name, column_value in uniqs:
                use_keys.append(column_name)
                use_values.append(column_value)

        elif len(uniqs) > 0:
            for column_name, column_value in uniqs:
                if not old_obj:
                    find = getattr(self, 'find_by_{}'.format(column_name))
                    old_obj = find(column_value)
                    if old_obj:
                        primarys = [[column_name, column_value]]
                        continue
                use_keys.append(column_name)
                use_values.append(column_value)

        if old_obj:
            part = ', '.join(['`{}`= ?'.format(k) for k in use_keys])
            where, values = parse_query(self._model.columns, primarys)
            for val in values:
                use_values.append(val)
            if len(use_values) < 2:
                logger.error('UPDATE {}'.format(str(obj)))
                return primarys[0][1]

            sql = 'UPDATE `{}` SET {} {}'.format(self._model.table_name, part, where)
            args = tuple(use_values)
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))

            cur.execute(sql, args)

            if self._model.auto_cache and conf.is_cache:
                self._del_cache(old_obj)

            return primarys[0][1]
        else:
            for primary in primarys:
                if primary[1] is not None:
                    use_keys.append(primary[0])
                    use_values.append(primary[1])

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

            if primarys and list(filter(lambda x: x[1] is not None, primarys)):
                return primarys[0][1]
            else:
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
            ret = unparse(ret, self._model.columns)
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

        return [self._model(self, unparse(ret, self._model.columns)) \
                for ret in _find_all()]

    def _del_cache(self, obj):
        obj = obj.copy()
        pri, uniqs = self._gen_cache_keys(obj)
        uniqs.append(pri)

        for mc_key in uniqs:
            mc.delete(mc_key)

    def _cache_set(self, obj):
        obj = obj.copy()
        pri, uniqs = self._gen_cache_keys(obj)
        uniqs.append(pri)

        for mc_key in uniqs:
            mc.set(mc_key, obj, self._model.cache_timeout)

    def _gen_cache_keys(self, obj):
        retval = {'pri': None, 'uniqs': []}
        uniqs = []
        pris = []
        for col in self._model.columns:
            if col.get('primary'):
                pris.append(col)
            elif col.get('unique'):
                uniqs.append(col)

        for col in uniqs:
            mc_key = mc.gen_key(self._model.table_name, col['name'], obj.get(col['name'], col.get('default')))
            retval['uniqs'].append(mc_key)

        cols = []
        for pri in pris:
            cols.append(pri['name'])
            cols.append(obj.get(pri['name'], pri.get('default')))

        if cols:
            mc_key = mc.gen_key(self._model.table_name, *cols)
            retval['pri'] = mc_key

        return retval['pri'], retval['uniqs']

    def del_all(self, query = None, limit = '', order = None, group = None,
            is_or = False):

        if self._model.auto_cache and conf.is_cache:
            uniqs = {}
            for col in self._model.columns:
                if col.get('unique') or col.get('primary'):
                    uniqs[col['name']] = col.get('default')
            column = ', '.join(map(lambda x: '`{}`'.format(x), uniqs.keys()))
            old_objs = self.find_all(query, column, limit, order, group, is_or)

            for old_obj in old_objs:
                self._del_cache(old_obj)

        where, values = parse_query(self._model.columns, query, limit, order, group,
                is_or)

        @_query(autocommit=True)
        def _del_all(cur):
            sql = 'DELETE FROM `{}` {}'.format(self._model.table_name, where)
            args = tuple(values)
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))

            cur.execute(sql, args)

        return _del_all()
