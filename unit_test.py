import pysql
import uuid
import datetime
columns = ['id', 'name', 'path', 'ctime']
key = ['id']


image_write = pysql.table('images', columns)
image_write.id = uuid.uuid1().get_hex()
image_write.name = 'test.jpg'
image_write.path = '/test/test.jpg'
image_write.ctime = datetime.datetime.now()
image_write.write()

image_iter = pysql.table('images', columns)

for i in image_iter:
    print 'Test write and __iter__', i.get_dict()

image_chain = pysql.table('images', columns)
image_chain.id = image_write.id
image_chain.chain(key)
print 'Test chain', image_chain.get_dict()


image_write.id = uuid.uuid1().get_hex()
image_write.name = 'test2.jpg'
image_write.path = '/test/test2.jpg'
image_write.ctime = datetime.datetime.now()
image_write.write()
image_delete = pysql.table('images', columns)
image_delete.id = image_write.id
image_delete.delete(key)
for i in image_iter:
    print 'Test delete key', i.get_dict()
image_delete.delete()
print 'Test delete all and counts', image_delete.counts()

