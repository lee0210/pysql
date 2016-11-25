# pysql
An simple interface to process data with mysql

Change the connetion arguments in the pysql.py

How to use:

table_definition.py:
    
    import pysql
    columns_1 = ['id', 'name', 'path', 'ctime'] 
    unique_key = ['id']
    key_1 = ['path']
    def get_without_lock():
        return plock.table('table_name', columns_1)
    def get_with_lock():
        return plock.lock('table_name', columns_1, unique_key)
        
db_processing.py:
    
    import files
    import uuid
    import datetime
    fs = files.get_without_lock()
    fs.id = uuid.uuid1().get_hex()
    fs.name = 'test update before image'
    fs.path = '/test'
    fs.ctime = datetime.datetime.now()
    fs.write()
    fs.name = 'test delete before image'
    fs.write()
    fs.path = '/test_other_key'
    fs.write()
    for f in fs:
        print f.get_dict()  
        if name == 'test update before image':
            f.name = 'test update after image'
            f.update(unique_key)
        if name == 'test delete before image':
            f.delete(unique_key)
    for f in fs:
        print f.get_dict()
    fs._key_list = key_1
    fs.path = '/test_other_key'
    for f in fs:
        print f.get_dict()
        

    
