from dbconf import configuare
import mysql.connector as DBConnector

__dbc = DBConnector.connect(**configuare)
__dbc.close()

def connect():
    if not __dbc.is_connected():
        __dbc.reconnect()
    return __dbc

def close():
    __dbc.close()
    return __dbc

def commit():
    __dbc.commit()
    return __dbc

def rollback():
    __dbc.rollback()
    return __dbc
def in_transaction():
    return __dbc.in_transaction
