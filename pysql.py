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
def get_dbc(commit=True):
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

type_list = [float, int, long]
attrs = ['__table__', '__columns__']


def SQL_Object(c_obj):
    global type_list, attrs
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
            setattr(self, 'f_'+k, v)

#
# Get the first matched record
#
    def chain(self):
        if not hasattr(c_obj, '__key__'):
            return False
        sql = """select `%s` from `%s` where %s"""%('`,`'.join(c_obj.__columns__.keys()), c_obj.__table__, ' and '.join(['`%s`=%%(%s)s'%(k, k) for k in c_obj.__key__]))
        if hasattr(c_obj, '__orderby__'):
            sql = sql + """ order by `%s`""" % '`,`'.join(c_obj.__orderby__)
        sql = sql + " limit 1 "

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
        sql = """select count(*) as `num` from `%s` where %s"""%(c_obj.__table__, ' and '.join(['`%s`=%%(%s)s'%(k, k) for k in c_obj.__key__]))
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
            sql = """update `%s` set %s where %s"""%(c_obj.__table__, ','.join(['`%s`=%%(%s)s'%(column, column) for column in c_obj.__columns__]), ' and '.join(['`%s`=%%(%s)s'%(k, k) for k in c_obj.__key__]))
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
        sql = """insert into `%s`(`%s`) values(%s)"""%(c_obj.__table__, '`,`'.join([column for column in c_obj.__columns__]), ','.join(['%%(%s)s'%column for column in c_obj.__columns__]))
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
            sql = """delete from `%s` where %s"""%(c_obj.__table__, ' and '.join(['`%s`=%%(%s)s'%(k, k) for k in c_obj.__key__]))
        else:
            sql = """delete from `%s`"""%c_obj.__table__
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
	        rtn[k] = getattr(self, 'f_'+k)
        return rtn
    def get_dict(self):
        rtn = {}
        for k in c_obj.__columns__:
            rtn[k] = getattr(self, 'f_'+k)
        return rtn
#
# iterator 
#
    def __iter__(self):
        sql = """select `%s` from `%s`""" % ('`,`'.join(c_obj.__columns__.keys()), c_obj.__table__)
        if hasattr(c_obj, '__key__'):
             sql = sql + """  where %s""" % ' and '.join(['`%s`=%%(%s)s'%(k, k) for k in c_obj.__key__])
        if hasattr(c_obj, '__orderby__'):
            sql = sql + """ order by `%s`""" % '`,`'.join(c_obj.__orderby__)
        if hasattr(self, 'page_size'):
            if not hasattr(self, 'cur_page'):
                self.cur_page = 1
            sql = sql + """ limit %d, %d"""  % ((self.cur_page - 1) * self.page_size, self.page_size)
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
    for k, v in c_obj.__columns__.items():
        k = 'f_' + k
        members[k] = v
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


