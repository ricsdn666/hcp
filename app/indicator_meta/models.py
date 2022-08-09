from common.connections import Base, engine
from sqlalchemy import Column, String, DateTime, JSON, Integer, Sequence
from datetime import datetime

Base.metadata.create_all(bind=engine)


class MetaOrm(Base):
    __tablename__: str = 'v3_indicator_meta'  # 数据表的表名
    id = Column(Integer, primary_key=True)
    indicator_id = Column(String(255))
    year = Column(String(255), default='', comment='年度')
    name = Column(String(255), comment='指标相关数据')
    value = Column(String(255), default='', comment='值')
    unit = Column(String(255), default='', comment='单位')
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class MetaHoleOrm(Base):
    __tablename__: str = 'v3_meta_hole'  # 数据表的表名
    id = Column(Integer, primary_key=True)
    indicator_id = Column(String(255))
    year = Column(String(255), default='', comment='年度')
    name = Column(String(255), comment='指标相关数据')
    value = Column(JSON, comment="下钻数据")
    unit = Column(String(255), default='', comment='单位')
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class MetaHoleYearDeptOrm(Base):
    __tablename__: str = 'v3_meta_hole_year_dept'  # 数据表的表名
    id = Column(Integer, primary_key=True)
    indicator_id = Column(String(255))
    year = Column(String(255), default='', comment='年度')
    name = Column(String(255), comment='指标相关数据')
    value = Column(JSON, comment="科室年度下钻数据")
    unit = Column(String(255), default='', comment='单位')
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class MetaHoleMonthDeptOrm(Base):
    __tablename__: str = 'v3_meta_hole_month_dept'  # 数据表的表名
    id = Column(Integer, primary_key=True)
    indicator_id = Column(String(255))
    year = Column(String(255), default='', comment='年度')
    name = Column(String(255), comment='指标相关数据')
    value = Column(JSON, comment="科室年度下钻数据")
    unit = Column(String(255), default='', comment='单位')
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)
