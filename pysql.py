#
# Simple DAO for mysql
#
# Author   Lee Li
# Contact  jw@qubitlee.com
#
from new import classobj
from exceptions import RuntimeError
from dbconf import configuare
import plock
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


class table(object):
    
    def __init__(self, table, columns, keys=None):
        self._table_name = table
        self._columns = columns
        self._key_list = keys
        self._order_by = None
        self._current_page = None
        self._page_size = None

    def set_value(self, d):
        for k, v in d.items():
            setattr(self, k, v)
#
# Get the first matched record
#
    def chain(self, keys=None):
        if self._key_list is None and keys is None:
            raise RuntimeError('No key found')
        if keys is None:
            keys = self._key_list
        sql = 'select `{0}` from `{1}` where {2}'.format(
            '`,`'.join(self._columns), 
            self._table_name, 
            ' and '.join(['`{0}`=%({0})s'.format(key) for key in keys]))
        if self._order_by is not None:
            sql = sql + ' order by `{0}`'.format('`,`'.join(self._order_by))
        sql = sql + ' limit 1 '

        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_keys(keys)))
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
        sql = 'select count(*) as `num` from `{0}`'.format(self._table_name)
        if self._key_list is not None or keys is not None:
             if keys is None:
                 keys = self._key_list
             sql = sql + '  where {0}'.format(' and '.join(['`{0}`=%({0})s'.format(key) for key in keys]))
        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_keys(keys)))
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
        if self._key_list is None and keys is None:
            raise RuntimeError('No key found')
        if keys is None:
            keys = self._key_list
        counts = self.counts(keys)
        if counts > 0:
            sql = 'update `{0}` set {1} where {2}'.format(
                self._table_name, 
                ','.join(['`{0}`=%({0})s'.format(column) for column in self._columns]), 
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
# write a record
#
    def write(self):
        sql = 'insert into `{0}`(`{1}`) values({2})'.format(
            self._table_name, 
            '`,`'.join(self._columns), 
            ','.join(['%({0})s'.format(column) for column in self._columns]))
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
        sql = 'delete from `{0}`'.format(
            self._table_name)
        if self._key_list is not None or keys is not None:
             if keys is None:
                 keys = self._key_list
             sql = sql + '  where {0}'.format(' and '.join(['`{0}`=%({0})s'.format(key) for key in keys]))
        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_keys(keys)))
            return True
        except Exception as e:
            print e
            return False
        finally:
            c.close()
            dbc.close()

    def get_keys(self, keys=None):
        if self._key_list is None and keys is None:
            return {}
        if keys is None:
            keys = self._key_list
        rtn = {}
	for key in keys:
	    rtn[key] = getattr(self, key)
        return rtn

    def get_dict(self):
        rtn = {}
        for column in self._columns:
            rtn[column] = getattr(self, column)
        return rtn
#
# iterator 
#
    def __iter__(self, keys=None):
        sql = 'select `{0}` from `{1}`'.format(
            '`,`'.join(self._columns), 
            self._table_name)
        if self._key_list is not None or keys is not None:
             if keys is None:
                 keys = self._key_list
             if len(keys) > 0:
                 sql = sql + '  where {0}'.format(' and '.join(['`{0}`=%({0})s'.format(key) for key in keys]))
        if self._order_by is not None:
            sql = sql + ' order by `{0}`'.format('`,`'.join(self._order_by))
        if self._page_size is not None:
            if self._current_page is None:
                self._current_page = 1
            sql = sql + ' limit {0}, {1}'.format(
                (self._current_page - 1) * self._page_size, 
                self._page_size)
        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_keys(keys)))
            for r in c:
                rec = self.__class__(self._table_name, self._columns)
                rec.set_value(r)
                yield rec
        except Exception as e:
            print e
            raise StopIteration
        finally:
            c.close()
            dbc.close()


class lock_table(table):
    def __init__(self, table, columns, unique_keys, keys=None):
        super(lock_table, self).__init__(table, columns, keys)
        self._unique_keys = unique_keys # use to lock record by record level
    def chain(self, keys=None):
        r = False
        if self.lock():
            r = super(lock_table, self).chain(keys)
            if not r: self.unlock()
        return r
    def delete(self, keys=None):
        r = super(lock_table, self).delete(keys)
        if r: self.unlock()
        return r
    def update(self, keys=None):
        r = super(lock_table, self).update(keys)
        if r: self.unlock()
        return r
    def lock(self):
        return plock.lock(self._table_name, self.get_keys(self._unique_keys))
    def unlock(self):
        return plock.unlock(self._table_name, self.get_keys(self._unique_keys))
    def __iter__(self, keys=None):
        sql = 'select `{0}` from `{1}`'.format(
            '`,`'.join(self._columns), 
            self._table_name)
        if self._key_list is not None or keys is not None:
             if keys is None:
                 keys = self._key_list
             if len(keys) > 0:
                 sql = sql + '  where {0}'.format(' and '.join(['`{0}`=%({0})s'.format(key) for key in keys]))
        if self._order_by is not None:
            sql = sql + ' order by `{0}`'.format('`,`'.join(self._order_by))
        if self._page_size is not None:
            if self._current_page is None:
                self._current_page = 1
            sql = sql + ' limit {0}, {1}'.format(
                (self._current_page - 1) * self._page_size, 
                self._page_size)
        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_keys(keys)))
            for r in c:
                rec = self.__class__(self._table_name, self._columns, self._unique_keys)
                rec.set_value(r)
                if rec.lock():
                    yield rec
        except Exception as e:
            print e
            raise StopIteration
        finally:
            c.close()
            dbc.close()
