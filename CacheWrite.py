#! /usr/bin/python
# -*- coding-utf8 -*-

import MySQLdb
import traceback
from DBUtils.PooledDB import PooledDB

configs = {
    'host': '127.0.0.1',
    'user':'vobile_cn',
    'passwd':'vobile_cn',
    'db':'test'
}

class DbManager(object):

    def __init__(self):
        conn_kwargs = {'host':configs['DB_HOST'], 'user':configs['DB_USER'], 'passwd':configs['DB_PASS'], 'db':configs['db']}
        self._pool = PooledDB(MySQLdb, mincached=0, maxcached=10, maxshared=10, maxusage=10000, **conn_kwargs)

    def getConn(self):
        return self._pool.connection()

class WriteCache(object):
    def __init__(self, conn, cache_max_num = 10):
        self.conn = conn
        self.cache_max_num = cache_max_num # num of sql to be write 
        self.sql_cache = []

    def insert(self, sql_str):
        if len(sql_cache) < cache_max_num:
            self.sql_cache.append(sql_str)
            return 0
        else:
            cursor = conn.cursor()
            try:
                for sql_item in self.sql_cache:
                    cursor.execute(sql_item)
                conn.commit()
                cursor.close()
                conn.close()
                self.sql_cache = [] # reset sql cache
            except:
                print 'insert failed'
                conn.rollback()
                traceback.print_exec()
                sys.exit(1) 
            else:
                return 0


if __name__ == "__main__":
    db_manager = DbManager()
    conn = db_manager.getConn()
    
    write_cache = WriteCache(conn, 3)
    sql_str1 = sql_str2 = sql_str3 = sql_str4 = ""; # just for example
    write_cache.insert(sql_str1);
    write_cache.insert(sql_str2);
    write_cache.insert(sql_str3);
    write_cache.insert(sql_str4);
