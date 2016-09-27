from lee import conf
import json
import pickle
import re
import logging

__all__ = ['unparse', 'parse', 'parse_query', 'logger', 'to_int', 'to_float',
    'to_str']

logger = logging.getLogger('lee')


def unparse(obj, columns):
    '''
    >>> types = ['int', 'float', 'str', 'bytes', 'bool', 'json', 'pickle']
    >>> columns = [{'name': 'test_%s'%t, 'type': t} for t in types]
    >>> obj = {'test_int': '2'}
    >>> unparse(obj, columns)
    {'test_int': 2}
    >>> unparse({'test_int': b'2', 'test_json': '{"key": "val"}'}, columns)
    {'test_json': {'key': 'val'}, 'test_int': 2}
    '''
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
                try:
                    obj[key] = _filter(tp, value,
                            column.get('encoding', 'UTF-8'))
                except Exception as e:
                    logger.exception(e)
                    obj[key] = None

    return obj

def parse(obj, columns):
    '''
    >>> types = ['int', 'float', 'str', 'bytes', 'bool', 'json', 'pickle']
    >>> columns = [{'name': 'test_%s'%t, 'type': t} for t in types]
    >>> obj = {'test_int': '2'}
    >>> parse(obj, columns)
    {'test_int': 2}
    >>> parse({'test_int': b'2', 'test_json': {"key": "val"}}, columns)
    {'test_json': '{"key": "val"}', 'test_int': 2}
    '''
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
                try:
                    obj[key] = _filter(tp, value,
                            column.get('encoding', 'UTF-8'))
                except Exception as e:
                    logger.exception(e)
                    obj[key] = None

    return obj

def _filter(tp, val, encoding = 'UTF-8'):
    '''
    >>> types = ['int', 'float', 'str', 'bytes', 'bool']
    '''
    if tp == 'int':
        val = to_int(val)

    elif tp == 'float':
        val = to_float(val)

    elif tp == 'str':
        val = to_str(val)

    elif tp == 'bytes':
        if not isinstance(val, bytes):
            val = to_str(val, encoding)
            val = bytes(val, encoding)

    elif tp == 'bool':
        if not conf.use_mysql:
            if isinstance(val, bool):
                val = to_int(val)

    return val

def to_int(val):
    '''
    >>> to_int(2)
    2
    >>> to_int('2')
    2
    >>> to_int(b'2')
    2
    >>> to_int(2.2)
    2
    >>> to_int(b'2.2')
    2
    >>> to_int(b'2.2')
    2
    >>> to_int(True)
    1
    >>> to_int(False)
    0
    >>> to_int(None)
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert None to int
    >>> to_int('a')
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert a to int
    >>> to_int(b'a')
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert b'a' to int
    '''
    if isinstance(val, (bytes, str)):
        if val.isdigit():
            return int(val)
        if isinstance(val, bytes):
            if val.count(b'.') == 1:
                if val.replace(b'.', b'').isdigit():
                    val = float(val)
                    return int(val)
        else:
            if val.count('.') == 1:
                if val.replace('.', '').isdigit():
                    val = float(val)
                    return int(val)
    if isinstance(val, (int, float, bool)):
        return int(val)

    raise ValueError('invalid: could not convert {} to int'.format(val))

def to_float(val):
    '''
    >>> to_float(2)
    2.0
    >>> to_float('2')
    2.0
    >>> to_float(b'2')
    2.0
    >>> to_float(2.2)
    2.2
    >>> to_float(b'2.2')
    2.2
    >>> to_float(b'2.2')
    2.2
    >>> to_float(True)
    1.0
    >>> to_float(False)
    0.0
    >>> to_float(None)
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert None to float
    >>> to_float('a')
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert a to float
    >>> to_float(b'a')
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert b'a' to float
    '''
    if isinstance(val, (bytes, str)):
        if val.isdigit():
            return float(val)
        if isinstance(val, bytes):
            if val.count(b'.') == 1:
                if val.replace(b'.', b'').isdigit():
                    return float(val)
        else:
            if val.count('.') == 1:
                if val.replace('.', '').isdigit():
                    return float(val)
    if isinstance(val, (int, float, bool)):
        return float(val)

    raise ValueError('invalid: could not convert {} to float'.format(val))

def to_str(val, encoding='UTF-8'):
    '''
    >>> to_str(2)
    '2'
    >>> to_str(2.2)
    '2.2'
    >>> to_str('2.2')
    '2.2'
    >>> to_str('str')
    'str'
    >>> to_str(b'str')
    'str'
    '''
    if isinstance(val, (bytes, bytearray)):
        return str(val, encoding)
    return str(val)

def parse_query(columns, query = None, limit = '', order = None, group = None,
        is_or = False):
    '''
    >>> types = ['int', 'float', 'str', 'bytes', 'bool', 'json', 'pickle']
    >>> columns = [{'name': 'test_%s'%t, 'type': t} for t in types]

    >>> query = {
    ...     'test_int': 2,
    ...     'test_str': 'this is a string',
    ...     'test_bytes': 'this is a bytes',
    ...     'test_json': {'key': 'val'},
    ... }
    ...
    >>> parse_query(columns, query)
    ('WHERE `test_int` = ? AND `test_str` = ? AND `test_bytes` = ? AND `test_json` = ?', [2, 'this is a string', b'this is a bytes', '{"key": "val"}'])

    >>> for op in 'gt|gte|lt|lte|eq|like'.split('|'):
    ...     out = parse_query(columns, {'test_int_$%s'%op: 2})
    ...     print("parse_query(columns, {'test_int_$%s': 2}) -> %s"%(op, out))
    ...
    parse_query(columns, {'test_int_$gt': 2}) -> ('WHERE `test_int` > ?', [2])
    parse_query(columns, {'test_int_$gte': 2}) -> ('WHERE `test_int` >= ?', [2])
    parse_query(columns, {'test_int_$lt': 2}) -> ('WHERE `test_int` < ?', [2])
    parse_query(columns, {'test_int_$lte': 2}) -> ('WHERE `test_int` <= ?', [2])
    parse_query(columns, {'test_int_$eq': 2}) -> ('WHERE `test_int` = ?', [2])
    parse_query(columns, {'test_int_$like': 2}) -> ('WHERE `test_int` LIKE "2"', [])

    >>> parse_query(columns, {'test_int_$in': [1, 2, 3]})
    ('WHERE `test_int` IN (?, ?, ?)', [1, 2, 3])

    >>> parse_query(columns, {'test_int_$notin': [1, 2, 3]})
    ('WHERE `test_int` NOT IN (?, ?, ?)', [1, 2, 3])

    >>> parse_query(columns, {'test_int': 1}, limit='1')
    ('WHERE `test_int` = ? LIMIT 1', [1])

    >>> parse_query(columns, [('test_str', 'str'), ('test_int', 1)])
    ('WHERE `test_int` = ? AND `test_str` = ?', [1, 'str'])

    >>> parse_query(columns, [('test_str', 'str'), ('test_int', 1)], is_or=True)
    ('WHERE `test_int` = ? OR `test_str` = ?', [1, 'str'])

    >>> parse_query(columns, {'test_int': 1}, limit='1, 2')
    ('WHERE `test_int` = ? LIMIT 1, 2', [1])

    >>> parse_query(columns, {'test_int': 1}, order = 'test_int')
    ('WHERE `test_int` = ? ORDER BY `test_int`', [1])

    >>> parse_query(columns, {'test_int': 1}, order = {'test_int': 'ASC'})
    ('WHERE `test_int` = ? ORDER BY `test_int` ASC', [1])

    >>> parse_query(columns, {'test_int': 1}, group = ['test_int'])
    ('WHERE `test_int` = ? GROUP BY `test_int`', [1])
    '''
    keys = []
    values = []
    where = []

    re_q = re.compile('^(.+?)_\$(gt|gte|lt|lte|eq|like|in|notin)$')
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
            elif s_op == 'notin':
                op = 'notin'

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
            elif op == 'notin':
                if len(val) > 0:
                    op = keys.append('`{}` NOT IN ({})'.format(key, ', '.join(['?'] * len(val))))
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
            _order = ['`{}` {}'.format(key, val) \
                    for key, val in order.items()]
        elif type(order) == list:
            if isinstance(order[0], (list, tuple)):
                _order = ['`{}` {}'.format(key, val) for key, val in order]
            else:
                _order = ['`{}`'.format(key) for key in order]
        else:
            _order = ['`{}`'.format(order)]

        where.append('ORDER BY {}'.format(', '.join(_order)))

    if group:
        _group = ['`{}`'.format(key) for key in group]
        where.append('GROUP BY {}'.format(', '.join(_group)))

    if limit:
        limit = str(limit)
        if not limit.startswith('limit'):
            limit = 'LIMIT {}'.format(limit)
        where.append(limit)

    return ' '.join(where), values

if __name__ == '__main__':
    import doctest
    doctest.testmod()
