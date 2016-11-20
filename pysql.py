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
        return True
    except Exception as e:
        print e
        return False
    finally:
        c.close()
        dbc.close()

attrs = ['__table__', '__columns__']


def SQL_Object(c_obj):
    global attrs
#
# Inherited from base table
#
    if hasattr(c_obj, '__basetable__'):
        p = c_obj.__basetable__
        if hasattr(p, '__basetable__'):
            c_obj.__basetable__ = p.__basetable__
        else:
            delattr(c_obj, '__basetable__')    
        for k in attrs:
            if not hasattr(c_obj, k) and hasattr(p, k):
                setattr(c_obj, k, getattr(p, k))
        return SQL_Object(c_obj)

    def set_value(self, d):
        for k, v in d.items():
            setattr(self, k, v)

#
# Get the first matched record
#
    def chain(self):
        if not hasattr(c_obj, '__key__'):
            return False
        sql = 'select `{0}` from `{1}` where {2}'.format(
            '`,`'.join(c_obj.__columns__), 
            c_obj.__table__, 
            ' and '.join(['`{0}`=%%({0})s'.format(k) for k in c_obj.__key__]))
        if hasattr(c_obj, '__orderby__'):
            sql = sql + ' order by `{0}`'.format('`,`'.join(c_obj.__orderby__))
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
    def counts(self):
        sql = 'select count(*) as `num` from `{0}` where {1}'.format(
            c_obj.__table__, 
            ' and '.join(['`{0}`=%%({0})s'.format(k) for k in c_obj.__key__]))
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
    def update(self):
        if not hasattr(c_obj, '__key__'):
            return False    
        counts = self.counts()
        if counts > 0:
            sql = 'update `{0}` set {1} where {2}'.format(
                c_obj.__table__, 
                ','.join(['`{0}`=%%({0})s'%(column) for column in c_obj.__columns__]), 
                ' and '.join(['`{0}`=%%({0})s'%(key) for key in c_obj.__key__]))
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
            c_obj.__table__, 
            '`,`'.join(c_obj.__columns__), 
            ','.join(['%%({0})s'.format(column) for column in c_obj.__columns__]))
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
    def delete(self):
        if hasattr(c_obj, '__key__'):
            sql = 'delete from `{0}` where {1}'.format(
                c_obj.__table__, 
                ' and '.join(['`{0}`=%%({0})s'.format(k) for k in c_obj.__key__]))
        else:
            sql = 'delete from `{0}`'.format(c_obj.__table__)
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
        if hasattr(c_obj, '__key__'):
	    for k in c_obj.__key__:
	        rtn[k] = getattr(self, k)
        return rtn

    def get_dict(self):
        rtn = {}
        for k in c_obj.__columns__:
            rtn[k] = getattr(self, k)
        return rtn
#
# iterator 
#
    def __iter__(self):
        sql = 'select `{0}` from `{1}`'.format(
            '`,`'.join(c_obj.__columns__), 
            c_obj.__table__)
        if hasattr(c_obj, '__key__'):
             sql = sql + '  where {0}'.format(' and '.join(['`%s`=%%(%s)s'%(k, k) for k in c_obj.__key__]))
        if hasattr(c_obj, '__orderby__'):
            sql = sql + ' order by `{0}`'.format('`,`'.join(c_obj.__orderby__))
        if hasattr(self, 'page_size'):
            if not hasattr(self, 'cur_page'):
                self.cur_page = 1
            sql = sql + ' limit {0}, {1}'.format(
                (self.cur_page - 1) * self.page_size, 
                self.page_size)
        try:
            dbc = get_dbc()
            c = dbc.cursor(dictionary=True)
            c.execute(sql, (self.get_keys()))
            for r in c:
                rec = self.__class__()
                rec.set_value(r)
                yield rec
        except Exception as e:
            print e
            raise StopIteration
        finally:
            c.close()
            dbc.close()
    members = {}
    members['chain'] = chain
    members['update'] = update
    members['delete'] = delete
    members['insert'] = insert
    members['get_keys'] = get_keys
    members['get_dict'] = get_dict
    members['__iter__'] = __iter__
    members['counts'] = counts
    members['set_value'] = set_value
    for k in attrs:
        members[k] = getattr(c_obj, k)
    return classobj(c_obj.__name__, (), members)

