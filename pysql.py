#
# Simple DAO for mysql
#
# Author   Lee Li
# Contact  jw@leezypig.com
#
from new import classobj
from exceptions import RuntimeError
from dbconf import configuare
import mysql.connector as DBConnector

#
# Connect to database server
#
def get_dbc():
    return DBConnector.connect(**configuare)
#
# Execute a SQL directly
#
def execute(sql):
    try:
        dbc = get_dbc()
        c = dbc.cursor()
        c.execute(sql)
        return c
    except Exception as e:
        print e
        return None
    finally:
        c.close()
        dbc.close()


class table:
    
    def __init__(self, table, columns):
        self.__table__ = table
        self.__columns__ = columns

    def set_value(self, d):
        for k, v in d.items():
            setattr(self, k, v)
#
# Get the first matched record
#
    def chain(self, keys=None):
        if not hasattr(self, '__keys__') and keys is None:
            return False
        if keys is None:
            keys = self.__keys__
        sql = 'select `{0}` from `{1}` where {2}'.format(
            '`,`'.join(self.__columns__), 
            self.__table__, 
            ' and '.join(['`{0}`=%({0})s'.format(key) for key in keys]))
        if hasattr(self, '__orderby__'):
            sql = sql + ' order by `{0}`'.format('`,`'.join(self.__orderby__))
        sql = sql + ' limit 1 '

        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_keys()))
            r = c.fetchone()
            if r == None:
                return False
            self.set_value(r)
            return True
        except Exception as e:
            print e
            return False
        finally:
            c.close()
            dbc.close()
#
#
#
    def counts(self, keys=None):
        if not hasattr(self, '__keys__') and keys is None:
            raise RuntimeError('No key found')
        if keys is None:
            keys = self.__keys__
        sql = 'select count(*) as `num` from `{0}` where {1}'.format(
            self.__table__, 
            ' and '.join(['`{0}`=%({0})s'.format(key) for key in keys]))
        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_dict()))
            r = c.fetchone()
            return r['num']
        except Exception as e:
            print e
            return 0
        finally:
            c.close()
            dbc.close()

#
# Update specific record
#
    def update(self, keys=None):
        if not hasattr(self, '__keys__') and keys is None:
            raise RuntimeError('No key found')
        if keys is None:
            keys = self.__keys__
        counts = self.counts(keys)
        if counts > 0:
            sql = 'update `{0}` set {1} where {2}'.format(
                self.__table__, 
                ','.join(['`{0}`=%({0})s'.format(column) for column in self.__columns__]), 
                ' and '.join(['`{0}`=%({0})s'.format(key) for key in keys]))
            try:
                dbc = get_dbc()
                c = dbc.cursor(dictionary=True)
                c.execute(sql, (self.get_dict()))
            except Exception as e:
                print e
            finally:
                c.close()
                dbc.close()
        return counts
#
# Insert a record
#
    def insert(self):
        sql = 'insert into `{0}`(`{1}`) values({2})'.format(
            self.__table__, 
            '`,`'.join(self.__columns__), 
            ','.join(['%({0})s'.format(column) for column in self.__columns__]))
        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_dict()))
            return True
        except Exception as e:
            print e
            return False
        finally:
            c.close()
            dbc.close()
#
# delete a record
#
    def delete(self, keys=None):
        if not hasattr(self, '__keys__') and keys is None:
            sql = 'delete from `{0}`'.format(self.__table__)
        else:
            if keys is None:
                keys = self.__keys__
            sql = 'delete from `{0}` where {1}'.format(
                self.__table__, 
                ' and '.join(['`{0}`=%({0})s'.format(k) for key in keys]))
        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_dict()))
            return True
        except Exception as e:
            print e
            return False
        finally:
            c.close()
            dbc.close()

    def get_keys(self):
        rtn = {}
        if hasattr(self, '__keys__'):
	    for key in self.__keys__:
	        rtn[key] = getattr(self, key)
        return rtn

    def get_dict(self):
        rtn = {}
        for column in self.__columns__:
            rtn[column] = getattr(self, column)
        return rtn
#
# iterator 
#
    def __iter__(self, keys=None):
        sql = 'select `{0}` from `{1}`'.format(
            '`,`'.join(self.__columns__), 
            self.__table__)
        if hasattr(self, '__keys__') or keys is not None:
             if keys is None:
                 keys = self.__keys__
             sql = sql + '  where {0}'.format(' and '.join(['`{0}`=%({0})s'.format(key) for key in keys]))
        if hasattr(self, '__orderby__'):
            sql = sql + ' order by `{0}`'.format('`,`'.join(self.__orderby__))
        if hasattr(self, '__page_size__'):
            if not hasattr(self, '__cur_page__'):
                self.__cur_page__ = 1
            sql = sql + ' limit {0}, {1}'.format(
                (self.__cur_page__ - 1) * self.__page_size__, 
                self.__page_size__)
        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_keys()))
            for r in c:
                rec = self.__class__(self.__table__, self.__columns__)
                rec.set_value(r)
                yield rec
        except Exception as e:
            print e
            raise StopIteration
        finally:
            c.close()
            dbc.close()

