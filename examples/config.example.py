import logging
FORMAT = '%(asctime)-15s - %(message)s'
logger = logging.getLogger('lee')
logger.setLevel(logging.DEBUG)
formater = logging.Formatter(FORMAT)
ch = logging.StreamHandler()
ch.setFormatter(formater)
logger.addHandler(ch)

mysql_path = 'mysql://127.0.0.1:3306?db=lee&user=&passwd='

sqlite_path = 'sqlite://run/cache.db'
