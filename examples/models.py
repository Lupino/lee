from lee import Table, Model, query
from lee.conf import use_mysql
from lee.logging import logger
from time import time

class Cache(Model):
    columns = [
        {'name': 'cache_id',   'type': 'str', 'primary': True, 'length': 32},
        {'name': 'value',      'type': 'pickle'},
        {'name': 'created_at', 'type': 'int', 'unsigned': True, 'default': lambda : int(time())}
    ]
    table_name = 'cache'

Cache = Table(Cache)

class Sequence(Model):
    table_name = 'sequence'

    columns = [
        {'name': 'name', 'type': 'str', 'primary': True, 'length': 20},
        {'name': 'id',   'type': 'int', 'default': 0}
    ]

    @query(autocommit=True)
    def next(self, name, cur):
        last_id = 0
        if use_mysql:
            sql = 'INSERT INTO `sequence` (`name`) VALUES (?) ON DUPLICATE KEY UPDATE `id` = LAST_INSERT_ID(`id` + 1)'
            args = (name, )
            logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))
            cur.execute(sql, args)
            last_id = cur.lastrowid
        else:
            seq = self._table.find_by_id(name)
            if seq:
                sql = 'UPDATE `sequence` SET `id` = `id` + 1 WHERE `name` = ?'
                args = (name, )
                logger.debug('Query> SQL: {} | ARGS: {}'.format(sql, args))
                cur.execute(sql, args)
            else:
                self._table.save({'name': name})

            seq = self._table.find_by_id(name)
            last_id = seq['id']
        return last_id

    def save(self, name, id):
        return self._table.save({'name': name, 'id': id})

seq = Table(Sequence)()

def main():
    cache = Cache({'cache_id': 'test', 'value': {'test': 'this is a test'}})
    cache.save()
    cache = Cache({'cache_id': 'test1', 'value': {'test': 'this is a test1'}})
    cache.save()

    caches = Cache.find_all()
    print(caches)
    caches[0]['value'] = {'test': 'this is a test2'}
    caches[0].save()
    caches = Cache.find_all()
    print(caches)
    print(list(map(lambda x: x.delete(), caches)))

    caches = Cache.find_all()
    print(caches)

    print(seq.next('test'))
    print(seq.next('test'))
    print(seq.next('test'))
    print(seq.next('test'))
    seq.save('test', 100)
    print(seq.next('test'))
    print(seq.next('test'))
    print(seq.next('test'))
