import sqlite3 as sqlite
from lee import conf
from lee.logging import logger

map_sqlite_types = {
    'str': 'TEXT',
    'int': 'INTEGER',
    'datetime': 'INTEGER',
    'json': 'TEXT',
    'pickle': '',
    'bool': 'INTEGER',
    'bytes': '',
    'text': 'TEXT',
    'float': 'REAL'
}

def dict_factory(cursor, row):
    retval = {}
    for idx, col in enumerate(cursor.description):
        value = row[idx]
        key = col[0]
        if key.startswith('`'):
            key = key[1:-1]
        retval[key] = value
    return retval


SQLITE_CONN=None
def _get_conn(conn=None):
    global SQLITE_CONN
    if SQLITE_CONN and not conn:
        conn = SQLITE_CONN

    if conn is None:
        conn = sqlite.connect(conf.path)

        conn.row_factory = dict_factory

        SQLITE_CONN = conn

    return conn

class query:
    def __init__(self, keyword='cur', autocommit=False):
        '''
            @keyword: default is cur
            @autocommit: if need commit set it True. default is False
        '''
        self._autocommit = autocommit
        self._keyword = keyword

    def __call__(self, fn):

        def warpper(*args, **kws):
            err_count = 0
            while err_count < 3:
                try:
                    conn = _get_conn()
                    cur = conn.cursor()
                    kws[self._keyword] = cur
                    ret = fn(*args, **kws)
                    if self._autocommit:
                        conn.commit()
                    return ret
                except sqlite.ProgrammingError as e:
                    if e.args[0].find('closed') > -1:
                        global SQLITE_CONN
                        SQLITE_CONN = None
                        err_count += 1
                    else:
                        break

                return None

        return warpper

@query()
def show_tables(cur):
    cur.execute('select `tbl_name` from `sqlite_master` where `type` = "table" ')
    cols = cur.fetchall()
    return list(map(lambda x: x['tbl_name'], cols))

@query()
def desc_table(table_name, cur):
    cur.execute('select `type`, `name`, `sql` from `sqlite_master` where `tbl_name` = ?', (table_name, ))
    cols = cur.fetchall()
    columns = {}
    for col in cols:
        if not col['sql']:
            continue
        start = col['sql'].find('(') + 1
        end = col['sql'].find(')')
        fields = col['sql'][start:end].replace('`', '').replace('"', '').replace("'", "")
        fields = [field.strip() for field in fields.split(',') if field.strip()]
        if col['type'] == 'table':
            for field in fields:
                column = {}
                field_list = [f for f in field.split(' ') if f]
                column['name'] = field_list[0]

                if len(field_list) >= 2:
                    column['type'] = field_list[1]

                field = field.lower()
                if field.find('unique') > -1:
                    column['unique'] = True

                if field.find('primary') > -1:
                    column['primary'] = True
                if column['name'] not in columns.keys():
                    columns[column['name']] = {}

                columns[column['name']].update(column)
        else:
            column = {}
            column['name'] = fields[0]
            column['index'] = True

            if column['name'] not in columns.keys():
                columns[column['name']] = {}

            columns[column['name']].update(column)
    return list(columns.values())

def diff_table(table_name, columns):
    '''
    only diff the column change unsupport index
    '''
    old_columns = desc_table(table_name)
    fields = list(map(lambda x: x['name'], old_columns))
    new_columns = []
    for column in columns:
        if column['name'] not in fields:
            new_columns.append(column)

    # drop_columns = list(old_columns.values())

    sql = []
    for column in new_columns:
        opts = gen_opts(column)
        sql.append('ALTER TABLE `{}` ADD `{}` {};'.format(table_name, column['name'], opts))
    # for column in drop_columns:
    #     sql.append('ALTER TABLE `{}` DROP COLUMN `{}`;'.format(table_name, column['name']))

    return sql

def gen_opts(column):
    tp = map_sqlite_types.get(column['type'])
    if tp is None:
        tp = column['type']

    default = column.get('default')
    if callable(default):
        default = None

    retval = ''
    retval += tp
    if column.get('unique') and not column.get('primary'):
        retval += ' UNIQUE'

    if default is not None and default != '':
        if column['type'] == 'bool':
            if default:
                default = 1
            else:
                default = 0

        retval += " DEFAULT '{}'".format(default)

    return retval

def gen_create_table_sql(table_name, columns):
    primarys = []
    column_sql = []
    index = []

    for column in columns:
        if column.get('primary'):
            primarys.append(column)
        else:
            opts = gen_opts(column)
            column_sql.append('`{}` {}'.format(column['name'], opts))
            if column.get('index') and not column.get('unique'):
                index.append(column)

    if primarys:
        if len(primarys) == 1:
            column = primarys[0]
            opts = gen_opts(column)
            column_sql.insert(0,
                    '`{}` {} PRIMARY KEY'.format(column['name'], opts))
        else:
            has_uniq = list(filter(lambda x: x.get('unique'), primarys))
            if has_uniq:
                if len(has_uniq) == 1:
                    column = has_uniq[0]
                    opts = gen_opts(column)
                    column_sql.insert(0,
                        '`{}` {} PRIMARY KEY'.format(column['name'], opts))
                else:
                    for column in has_uniq:
                        opts = gen_opts(column)
                        column_sql.append('`{}` {} UNIQUE'.format(column['name'], opts))

            for column in primarys:
                if not column.get('unique'):
                    opts = gen_opts(column)
                    column_sql.append('`{}` {}'.format(column['name'], opts))
                    index.append(column)
    sql = ['create table if not exists `{}` ({})'.format(table_name,
            ', '.join(column_sql))]

    for column in index:
        sql.append('create index if not exists' + \
                ' {0}_{1}_index on `{0}`(`{1}`)'.format(\
                    table_name, column['name']))

    return sql

@query(autocommit=True)
def create_table(table_name, columns, cur):
    for sql in gen_create_table_sql(table_name, columns):
        logger.debug('Query> SQL: {}'.format(sql))
        cur.execute(sql)
