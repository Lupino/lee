from lee import conf
import json
import pickle
import re
import logging

logger = logging.getLogger('lee')

__all__ = ['unparse', 'parse', 'parse_query']

def unparse(obj, columns):
    for column in columns:
        if obj.get(column['name']) is not None:
            key = column['name']
            value = obj[key]
            tp = column['type']

            if tp == 'json':
                try:
                    if value and isinstance(value, str):
                        obj[key] = json.loads(value)
                except Exception as e:
                    logger.exception(e)
                    obj[key] = None

            elif tp == 'pickle':
                try:
                    if value and isinstance(value, bytes):
                        obj[key] = pickle.loads(value)
                except Exception as e:
                    logger.exception(e)
                    obj[key] = None

            else:
                obj[key] = _filter(tp, value,
                        column.get('encoding', 'UTF-8'))

    return obj

def parse(obj, columns):
    for column in columns:
        if obj.get(column['name']) is not None:
            key = column['name']
            value = obj[key]
            tp = column['type']

            if tp == 'json':
                try:
                    if value:
                        obj[key] = json.dumps(value)
                except Exception as e:
                    logger.exception(e)
                    obj[key] = None

            elif tp == 'pickle':
                try:
                    if value:
                        obj[key] = pickle.dumps(value)
                except Exception as e:
                    logger.exception(e)
                    obj[key] = None

            else:
                obj[key] = _filter(tp, value,
                        column.get('encoding', 'UTF-8'))

    return obj

def _filter(tp, val, encoding = 'UTF-8'):
    if tp == 'int':
        if not isinstance(val, int):
            if re.match('^[0-9-]+$', str(val)):
                val = int(val)
            else:
                val = None

    elif tp == 'float':
        if not isinstance(val, float):
            if re.match('^[0-9.-]+$', str(val)):
                val = float(val)
            else:
                val = None

    elif tp == 'str':
        if not isinstance(val, str):
            if isinstance(val, (int, float)):
                val = str(val)
            else:
                val = str(val, encoding)

    elif tp == 'bytes':
        if not isinstance(val, bytes):
            if isinstance(val, (int, float)):
                val = str(val)
            val = bytes(val, encoding)
    elif tp == 'bool':
        if not use_mysql:
            if isinstance(val, bool):
                if val:
                    val = 1
                else:
                    val = 0

    return val

def parse_query(columns, query = None, limit = '', order = None, group= None,
        is_or = False):
    keys = []
    values = []
    where = []

    re_q = re.compile('^(.+?)_\$(gt|gte|lt|lte|eq|like|in)$')
    def parse_item(item):
        if len(item) == 3:
            return item
        else:
            key, val = item
        q = re_q.search(key)
        op = '='
        if q:
            key = q.group(1)
            s_op = q.group(2)
            s_op = s_op.lower()
            if s_op == 'gt':
                op = '>'
            elif s_op == 'gte':
                op = '>='
            elif s_op == 'lt':
                op = '<'
            elif s_op == 'lte':
                op = '<='
            elif s_op == 'eq':
                op = '='
            elif s_op == 'like':
                op = 'like'
            elif s_op == 'in':
                op = 'in'

        return key, op, val

    cols = list(map(lambda x: x['name'], columns))
    def get_order(query):
        key = query[0]
        if key not in cols:
            cols.append(key)
        return cols.index(key)

    if query:
        if isinstance(query, dict):
            query = query.items()

        queries = list(map(parse_item, query))
        queries = sorted(queries, key=get_order)

        for key, op, val in queries:
            if op == 'like':
                keys.append('`{}` LIKE "{}"'.format(key, val))
            elif op == 'in':
                if len(val) > 0:
                    op = keys.append('`{}` IN ({})'.format(key, ', '.join(['?'] * len(val))))
                    for v in val:
                        values.append(v)
            else:
                if op == '=':
                    val = parse({key: val}, columns)[key]
                keys.append('`{}` {} ?'.format(key, op))
                values.append(val)

        if len(keys) > 0:
            where.append('WHERE')
            if is_or:
                where.append(' OR '.join(keys))
            else:
                where.append(' AND '.join(keys))

    if order:
        if type(order) == dict:
            _order = ['ORDER BY `{}` {}'.format(key, val) \
                    for key, val in order.items()]
        elif type(order) == list:
            _order = ['ORDER BY `{}`'.format(key) for key in order]
        else:
            _order = ['ORDER BY `{}`'.format(order)]

        where.extend(_order)

    if group:
        _group = ['GROUP BY `{}`'.format(key) for key in group]
        where.extend(_group)

    if limit:
        limit = str(limit)
        if not limit.startswith('limit'):
            limit = 'LIMIT {}'.format(limit)
        where.append(limit)

    return ' '.join(where), values
