# -*- coding: utf-8 -*-
#
# mysql，redis等连接函数
# Author: wonderful
# Email: 86531130@qq.com
# Created Time: 2022-05-17
from typing import Iterator
from redis import Redis, ConnectionPool

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# redis pool
_redis_pool = None

# SQLALCHEMY_DATABASE_URL = 'mysql+pymysql://root:Root@123..@123.56.203.37:3306/disease'
SQLALCHEMY_DATABASE_URL = 'mysql+pymysql://root:@127.0.0.1:3306/third-level-review'
SQLALCHEMY_DATABASE_URL = "postgresql://ecrf:ecrf2021.com@221.2.94.162:25432/linyidb"  # MySQL或PostgreSQL的连接方法

engine = create_engine(
    # echo=True表示引擎将用repr()函数记录所有语句及其参数列表到日志
    # 由于SQLAlchemy是多线程，指定check_same_thread=False来让建立的对象任意线程都可使用。这个参数只在用SQLite数据库时设置
    # SQLALCHEMY_DATABASE_URL, encoding='utf-8', echo=True, connect_args={'check_same_thread': False}
    SQLALCHEMY_DATABASE_URL, encoding='utf-8', echo=True
)

# 在SQLAlchemy中，CRUD都是通过会话(session)进行的，所以我们必须要先创建会话，每一个SessionLocal实例就是一个数据库session
# flush()是指发送数据库语句到数据库，但数据库不一定执行写入磁盘；commit()是指提交事务，将变更保存到数据库文件
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=True)

# 创建基本映射类
Base = declarative_base(bind=engine, name='Base')

Base.metadata.create_all(bind=engine)


# Base.metadata.create_all(bind=engine)
def init_redis(host: str, port=6379, db=0):
    """配置redis """
    global _redis_pool
    _redis_pool = ConnectionPool(host=host, port=port, db=db)


def get_redis() -> Iterator[Redis]:
    """获取redis操作对象
    每一个请求处理完毕后会关闭当前连接，不同的请求使用不同的连接
    """
    # 检查间隔(health_check_interval)的含义:
    # 当连接在health_check_interval秒内没有使用下次使用时需要进行健康检查。
    # 在内部是通过发送ping命令来实现
    r = Redis(connection_pool=_redis_pool, health_check_interval=30)
    try:
        yield r
    finally:
        r.close()


if __name__ == '__main__':
    import sys

    init_redis(sys.argv[1])
    r = get_redis()
    r = next(r)
    print(r)
    r.set('key', 'val', 10)
    assert str(r.get('key'), encoding='utf8') == 'val'
