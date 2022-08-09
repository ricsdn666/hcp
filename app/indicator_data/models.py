from common.connections import Base
from sqlalchemy import Column, String, DateTime,JSON,Integer
from datetime import datetime


class IndicatorOrm(Base):
    __tablename__: str = 'v3_indicator'  # 数据表的表名
    indicator_id = Column(String(255), primary_key=True)
    root_id = Column(String(255), comment='章节')
    year = Column(String(255), default='', comment='年度')
    name = Column(String(255), comment='指标名称')
    indicator_property = Column(String(255), default='', comment='指标属性')
    unit = Column(String(255), comment='计量单位')
    process_method = Column(String(255), default='', comment='计算方法')
    direction = Column(String(255), default='', comment='指标导向')
    data_source = Column(String(255), default='', comment='数据来源')
    value = Column(String(255), default='', comment='指标值')
    js_value = Column(JSON, default='', comment='json格式指标值')
    is_hole = Column(Integer,default=0,comment='是否下钻')
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class IndCategoryOrm(Base):
    __tablename__: str = 'v3_ind_category'  # 数据表的表名
    c_id = Column(String(255), primary_key=True, comment='指标分类id')
    name = Column(String(255), comment='分类名称')
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)
