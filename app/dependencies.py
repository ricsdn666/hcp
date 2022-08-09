# -*- coding: utf-8 -*-
#
# 依赖项
# Author: wonderful
# Email: 86531130@qq.com
# Created Time: 2022-05-17


from common.connections import engine, Base, SessionLocal


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
