# pysql
An simple interface to process data with mysql

Change the connetion arguments in the pysql.py

How to use:

files.py:
from pysql import table
columns_1 = ['id', 'name', 'path', 'ctime'] 
key_1 = ['id']
def get():
    return table('files', columns_1)
    
program_to_read_database.py:
import files
fs = files.get()
for f in fs:
    print f.get_dict()
    
program_to_update_database.py:
import files
from datetime import datetime
fs = files.get()
for f in fs:
    f.ctime = datetime.now()
    f.update(files.key_1)
