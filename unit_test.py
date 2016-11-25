import pysql
import plock
import uuid
import datetime
columns = ['id', 'name', 'path', 'ctime']
columns2 = ['name', 'ctime']
key = ['id']

image_write = pysql.table('images', columns)
for i in range(0, 5):
    image_write.id = uuid.uuid1().get_hex()
    image_write.name = 'test{0}.jpg'.format(i)
    image_write.path = '/test/test{0}.jpg'.format(i)
    image_write.ctime = datetime.datetime.now()
    image_write.write()

image_iter = pysql.table('images', columns)
image_iter2 = pysql.table('images', columns2)

for i in image_iter:
    print 'Test write and __iter__', i.get_dict()
for i in image_iter2:
    print 'Test write and __iter__2', i.get_dict()

image_update = pysql.table('images', columns, key)
image_update.id = image_write.id
image_update.chain()
image_update.name = 'test update'
image_update.update()

image_chain = pysql.table('images', columns)
image_chain.id = image_write.id
image_chain.chain(key)
print 'Test chain', image_chain.get_dict()

image_delete = pysql.table('images', columns)
image_delete.id = image_write.id
image_delete.delete(key)
for i in image_iter:
    print 'Test delete the last', i.get_dict()
image_delete.delete()
print 'Test delete all and counts', image_delete.counts()

lock_image = pysql.lock_table('images', columns, key)
for i in range(0, 10):
    image_write.id = uuid.uuid1().get_hex()
    image_write.name = 'lock_test{0}.jpg'.format(i%2)
    image_write.path = '/lock_test/lock_test{0}.jpg'.format(i)
    image_write.ctime = datetime.datetime.now()
    image_write.write()

lock_image._key_list = ['name']
lock_image.name = 'lock_test1.jpg'
for i in lock_image:
    print 'Test lock __iter__', i.get_dict()
    plock.print_lock()
    i.delete(key)
    plock.unlock('images')
image_delete.delete()
