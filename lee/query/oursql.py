import oursql
from lee.conf import mysql as _mysql
from lee.utils import logger

__all__ = ['query', 'create_table', 'show_tables', 'diff_table', 'desc_table']

map_mysql_types = {
    'str': 'VARCHAR',
    'int': 'INT',
    'datetime': 'TIMESTAMP',
    'json': 'VARCHAR',
    'pickle': 'BLOB',
    'bool': 'TINYINT',
    'bytes': 'BLOB',
    'text': 'TEXT',
    'float': 'FLOAT'
}

MYSQL_CONN=None
def _get_conn(conn=None):
    global MYSQL_CONN
    if MYSQL_CONN and not conn:
        conn = MYSQL_CONN

    if conn:
        try:
            conn.ping();
        except oursql.OperationalError as e:
            logger.exception(e)
            conn = None

    if conn is None:
        conn = oursql.connect(
                    host = _mysql['host'],
                    port = _mysql['port'],
                    db = _mysql['db'],
                    user = _mysql['user'],
                    passwd = _mysql['passwd'],
                    default_cursor = oursql.DictCursor,
                    use_unicode = True
                )
        MYSQL_CONN = conn
    return conn

class query:
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
            # Connect to the database
            rv = None

            if self.keyword not in kwargs.keys():

                conn = _get_conn()
                cur = conn.cursor()

                # Add the connection handle as a keyword argument.
                kwargs[self.keyword] = cur
            else:
                conn = kwargs[self.keyword].connection
                conn = _get_conn(conn)

            try:
                rv = callback(*args, **kwargs)
                if self.autocommit:
                    conn.commit()
            except oursql.IntegrityError as e:
                logger.exception(e)
                conn.rollback()
                #raise e
            except oursql.OperationalError as e:
                logger.exception(e)
                #raise e
            except oursql.CollatedWarningsError as e:
                logger.exception(e)

            return rv

        return wrapper

@query()
def show_tables(cur):
    cur.execute('show tables')
    cols = cur.fetchall()
    return list(map(lambda x: list(x.values())[0], cols))

@query()
def desc_table(table_name, cur):
    cur.execute('desc `{}`'.format(table_name))
    cols = cur.fetchall()
    # {'Type': 'varchar(50)', 'Default': None, 'Field': 'name', 'Null': 'NO', 'Key': 'PRI', 'Extra': ''}
    columns = []
    for col in cols:
        column = {}
        left_bracket = col['Type'].find('(')
        right_bracket = col['Type'].find(')')
        if left_bracket > -1:
            column['type'] = col['Type'][:left_bracket].upper()
            length = col['Type'][left_bracket+1:right_bracket]
            if length.isnumeric():
                length = int(length)
            column['length'] = length
        else:
            column['type'] = col['Type'].split(' ')[0]

        if col['Type'].find('unsigned') > -1:
            column['unsigned'] = True

        if col['Null'] == 'YES':
            column['null'] = True

        if col['Default']:
            column['default'] = col['Default']
            column['null'] = True

        column['name'] = col['Field']

        if col['Extra'] == 'auto_increment' or col['Extra'].find('auto_increment') > -1:
            column['auto_increment'] = True

        if col['Key'] == 'PRI' or col['Key'].find('PRI') > -1:
            column['primary'] = True

        if col['Key'] == 'UNI' or col['Key'].find('UNI') > -1:
            column['unique'] = True

        if col['Key'] == 'MUL' or col['Key'].find('MUL') > -1:
            column['index'] = True

        columns.append(column)

    return columns

def diff_table(table_name, columns):
    '''
    diff the column change
    '''
    old_columns = desc_table(table_name)
    old_columns = dict(map(lambda x: (x['name'], x), old_columns))
    diff_columns = []
    new_columns = []
    diff_index_columns = []

    for column in columns:
        old_column = old_columns.pop(column['name'], None)
        if not old_column:
            new_columns.append(column)
            continue

        old_length = old_column.get('length')
        tp = map_mysql_types.get(column['type'])
        if not tp:
            tp = column['type']
        length = column.get('length')
        unsigned = column.get('unsigned')
        if not length:
            if tp == 'INT':
                if unsigned:
                    length = 10
                else:
                    length = 11

            if tp == 'TINYINT':
                length = 1

        old_auto_incr = old_column.get('auto_increment', False)
        auto_incr = column.get('auto_increment', False)
        old_is_null = old_column.get('null', False)
        is_null = column.get('null', False)
        old_default = column.get('default', None)
        default = column.get('default', None)
        if default is not None:
            is_null = True

        if callable(default):
            default = None
            old_default = None
            old_is_null = True

        if auto_incr:
            default = None
            old_default = None
            is_null = True
            old_is_null = True


        if old_length != length or old_auto_incr != auto_incr or \
                old_is_null != is_null or default != old_default:
            diff_columns.append(column)

        if old_column.get('primary', False) != column.get('primary', False):
            diff_index_columns.append((old_column, column))

        elif old_column.get('unique', False) != column.get('unique', False):
            if not column.get('primary'):
                diff_index_columns.append((old_column, column))

        elif old_column.get('index', False) != column.get('index', False):
            if not column.get('primary') and not column.get('unique'):
                diff_index_columns.append((old_column, column))

    drop_columns = list(old_columns.values())

    sql = []
    for column in new_columns:
        opts = gen_opts(column)
        sql.append('ALTER TABLE `{}` ADD `{}` {};'.format(table_name, column['name'], opts))
    for column in drop_columns:
        sql.append('ALTER TABLE `{}` DROP COLUMN `{}`;'.format(table_name, column['name']))
    for column in diff_columns:
        opts = gen_opts(column)
        sql.append('ALTER TABLE `{}` MODIFY COLUMN `{}` {};'.format(table_name, column['name'], opts))

    for old_column, column in diff_index_columns:
        if old_column.get('primary') != column.get('primary'):
            if old_column.get('primary'):
                sql.append('ALTER TABLE `{}` DROP PRIMARY KEY'.format(table_name))
            else:
                sql.append('ALTER TABLE `{}` ADD PRIMARY KEY (`{}`)'.format(table_name, column['name']))

        elif old_column.get('unique') != column.get('unique'):
            if old_column.get('unique'):
                sql.append('ALTER TABLE `{}` DROP INDEX `{}`'.format(table_name, column['name']))
            else:
                sql.append('ALTER TABLE `{}` ADD  UNIQUE INDEX `{}` (`{}`)'.format(table_name, column['name'], column['name']))

        else:
            if old_column.get('index'):
                sql.append('ALTER TABLE `{}` DROP INDEX `{}`'.format(table_name, column['name']))
            else:
                sql.append('ALTER TABLE `{}` ADD  INDEX `{}` (`{}`)'.format(table_name, column['name'], column['name']))

    return sql

def gen_opts(column):
    tp = map_mysql_types.get(column['type'])
    if not tp:
        tp = column['type']
    max_length = column.get('length')
    default = column.get('default')
    if callable(default):
        default = None
    is_null = column.get('null')
    auto_increment = column.get('auto_increment')
    unsigned = column.get('unsigned')

    if not max_length:
        if tp == 'VARCHAR':
            tp = 'TEXT'

        if tp == 'INT':
            if unsigned:
                max_length = 10
            else:
                max_length = 11

        if tp == 'TINYINT':
            max_length = 1

    retval = ''
    retval += tp
    if max_length:
        retval += '({})'.format(max_length)
    if unsigned:
        retval += ' UNSIGNED'
    if is_null:
        retval += ' DEFAULT NULL'
    elif default is not None and default != '':

        if column['type'] == 'bool':
            if default:
                default = 1
            else:
                default = 0

        retval += " DEFAULT '{}'".format(default)
    else:
        retval += ' NOT NULL'

    if auto_increment:
        retval += ' AUTO_INCREMENT'

    return retval

def gen_create_table_sql(table_name, columns, spec_index, spec_uniq):
    primarys = []
    uniqs = []
    index = []
    column_sql = []
    for column in columns:
        if column.get('primary'):
            primarys.append(column)
        elif column.get('unique'):
            uniqs.append(column)
        elif column.get('index'):
            index.append(column)
        opts = gen_opts(column)
        column_sql.append('`{}` {}'.format(column['name'], opts))

    primarys = list(map(lambda x: '`{}`'.format(x['name']), primarys))
    uniqs = list(map(lambda x: '`{}`'.format(x['name']), uniqs))
    index = list(map(lambda x: '`{}`'.format(x['name']), index))
    column_sql.append('PRIMARY KEY ({})'.format(', '.join(primarys)))
    for uniq in uniqs:
        column_sql.append('UNIQUE KEY {} ({})'.format(uniq, uniq))

    for uniq in spec_uniq:
        column_sql.append('UNIQUE KEY `{}` (`{}`)'.format(uniq[0], '`, `'.join(uniq[1:])))

    for idx in index:
        column_sql.append('KEY {} ({})'.format(idx, idx))

    for idx in spec_index:
        column_sql.append('KEY `{}` (`{}`)'.format(idx[0], '`, `'.join(idx[1:])))

    return 'CREATE TABLE IF NOT EXISTS `{}` ({})'.format(\
            table_name, ', '.join(column_sql)) +\
            ' ENGINE=InnoDB DEFAULT CHARSET=utf8;'
@query(autocommit=True)
def create_table(table_name, columns, spec_index, spec_uniq, cur):
    sql = gen_create_table_sql(table_name, columns, spec_index, spec_uniq)
    logger.debug('Query> SQL: {}'.format(sql))
    cur.execute(sql)
