#http://lib.csdn.net/snippet/python/47292


import asyncio, logging

import aiomysql

def log(sql, args=()):
    logging.info('SQL: %s' % sql)

# 创建连接池,每个http请求都从连接池连接到数据库
@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

# 销毁连接池
@asyncio.coroutine
def destory_pool():
    global __pool
    if __pool is not None:
        __pool.close()
        yield from __pool.wait_closed()

# select语句
@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor) ## 打开一个DictCursor，它与普通游标的不同在于，以dict形式返回结果
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

# insert,update,deleta语句
@asyncio.coroutine
def execute(sql, args):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        try:
            # execute类型的SQL操作返回的结果只有行号，所以不需要用DictCursor
            # execute()函数和select()函数所不同的是,cursor对象不返回结果集，而是通过rowcount返回结果数
            cur = yield from conn.cursor()
            cur.execute(sql.replace('?', '%s'), args)
            affectedLine = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affectedLine

# 根据输入的参数生成占位符列表
def create_args_string(num):
    l = []
    for n in range(num):
        l.append('?')
    # 以','为分隔符，将列表合成字符串
    return ', '.join(l)


# 定义Field类，负责保存(数据库)表的字段名和字段类型
class Field(object):
    # 表的字段包含名字、类型、是否为表的主键和默认值
    def __init__(self, name, colunm_type, primary_key, default):
        self.name = name
        self.colunm_type = colunm_type
        self.primary_key = primary_key
        self.default = default

    # 当打印(数据库)表时，输出(数据库)表的信息:类名，字段类型和名字
    def __str__(self):
        return '<%s, %s, %s>' % (self.__class__.__name__, self.colunm_type, self.name)

# -*- 定义不同类型的衍生Field -*-
# -*- 表的不同列的字段的类型不一样

class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


# -*-定义Model的元类
# 关于元类 http://blog.jobbole.com/21351/

class ModelMetaclass(type):
    # 调用__init__方法前会调用__new__方法
    #__init__是在类实例创建之后调用，而 __new__方法正是创建这个类实例的方法
    #__new__方法默认返回实例对象供__init__方法、实例方法使用
    # 参数：1.当前准备创建的类的对象  2.类的名字 3.类继承的父类集合 4.类的方法集合
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 如果没设置__table__属性，tablename就是类的名字
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings = {}
        fields = []
        primarykey = None
        # 键是列名，值是field子类

        # 在当前类中查找所有的类属性(attrs)，如果找到Field属性，
        # 就将其保存到__mappings__的dict中，
        # 同时从类属性中删除Field(防止实例属性遮住类的同名属性)
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                # 把键值对存入mapping字典中
                mappings[k] = v
                # 如果此时类实例的已存在主键，说明主键重复了(一个实例只能有一个主键)
                if v.primary_key:
                    #找到主键
                    if primarykey:
                        raise Exception('Duplicate primary key for field: %s' % k)
                    primarykey = k
                # 所以主键是没有存到field的
                else:
                    fields.append(k)
        # end for

        if not primarykey:
            raise Exception('Primary key not found.')
        # 删除类属性
        for k in mappings.keys():
            attrs.pop(k)

        #map用法
        # >> > def add100(x):
        #     return x + 100
        # >> > hh = [11, 22, 33]
        # >> > map(add100, hh)
        # [111, 122, 133]

        # 保存除主键外的属性名为``（运算出字符串）列表形式
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primarykey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        # 反引号和repr()函数功能一致
        # repr()将对象转化为供解释器读取的形式
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primarykey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
        tableName, ', '.join(escaped_fields), primarykey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
        tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primarykey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primarykey)
        return type.__new__(cls, name, bases, attrs)

# 这样，任何继承自Model的类（比如User），
# 会自动通过ModelMetaclass扫描映射关系，
# 并存储到自身的类属性如__table__、__mappings__中。
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super().__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        #返回对象的属性,如果没有对应属性则会调用__getattr__
        return getattr(self, key, None)

    #对于默认值的处理
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                #value = callable(field.default)?field.default():field.default
                #应当是看这个列的默认值是个函数还是值吧，如果是函数就调用函数，是值就直接赋值
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                # 把默认属性设置进去
                setattr(self, key, value)
        return value

    @classmethod
    # 类方法有类变量cls传入，从而可以用cls做一些相关的处理。
    # 并且有子类继承时，调用该类方法时，传入的类变量cls是子类，而非父类。
    # 类方法的第一个参数是cls,而实例方法的第一个参数是self
    @asyncio.coroutine
    def findAll(cls, where=None, args=None, **kw):
        '''find objects by where clause'''
        sql = [cls.__select__]

        if where:
            sql.append('where')
            sql.append(where)

        if args is None:
            args = []
        #D.get(k[,d]) -> D[k] if k in D, else d
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)

        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?,?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = yield from select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    @asyncio.coroutine
    def findNumber(cls, selectField, where=None, args=None):
        '''find number by select and where.'''
        sql = ['select %s __num__ from `%s`' %(selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = yield from select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['__num__']

    @classmethod
    @asyncio.coroutine
    def find(cls, primarykey):
        '''find object by primary key'''
        rs = yield from select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [primarykey], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    @asyncio.coroutine
    def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = yield from execute(self.__updata__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    @asyncio.coroutine
    def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = yield from execute(self.__updata__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)


#使用
# if __name__ == '__main__':
#     class User(Model):
#         # 定义类的属性到列的映射：
#         id = IntegerField('id', primary_key=True)
#         name = StringField('username')
#         email = StringField('email')
#         password = StringField('password')
#
#     # 创建一个实例：
#     user = User(id=123, name='Michael')
#     yield from user.save(）
#     u.save()没有任何效果，因为调用save()仅仅是创建了一个协程，并没有执行它。
#     一定要用 yield from user.save(） 才真正执行了INSERT操作。
