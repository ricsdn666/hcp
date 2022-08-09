from typing import List

from sqlalchemy.orm import Session

from .models import MetaHoleMonthDeptOrm
from .models import MetaHoleOrm
from .models import MetaHoleYearDeptOrm
from .models import MetaOrm


def get_meta_by_indicator_id(db: Session, indicator_id: str):
    return db.query(MetaOrm).filter(MetaOrm.indicator_id == indicator_id).all()


def get_meta_by_name(db: Session, name: str, year: str):
    return db.query(MetaOrm).filter(MetaOrm.name == name, MetaOrm.year == year).first()


def get_meta_hole_by_indicator_id(db: Session, indicator_id: str):
    return db.query(MetaHoleOrm).filter(MetaHoleOrm.indicator_id == indicator_id).order_by(MetaHoleOrm.id).all()


def get_meta_year_dept_hole_by_indicator_id(db: Session, indicator_id: str, year: str):
    return db.query(MetaHoleYearDeptOrm).filter(MetaHoleYearDeptOrm.indicator_id == indicator_id,
                                                MetaHoleYearDeptOrm.year == year).order_by(MetaHoleYearDeptOrm.id).all()


def get_meta_year_dept_hole_by_meta_name(db: Session, meta_name: str, year: str):
    return db.query(MetaHoleYearDeptOrm).filter(MetaHoleYearDeptOrm.name == meta_name,
                                                MetaHoleYearDeptOrm.year == year).order_by(MetaHoleYearDeptOrm.id).all()


def get_meta_month_dept_hole_by_meta_name(db: Session, meta_name: str, year: str):
    return db.query(MetaHoleMonthDeptOrm).filter(MetaHoleMonthDeptOrm.name == meta_name,
                                                 MetaHoleMonthDeptOrm.year == year).order_by(
        MetaHoleMonthDeptOrm.id).all()


def update(db: Session, ms: List[MetaOrm]):
    for m in ms:
        db.query(MetaOrm).filter(MetaOrm.id == m.id).update(m.dict())
    db.commit()


'''
def get_indicator_by_root_id(db: Session, root_id: str):
    return db.query(IndicatorOrm).filter(IndicatorOrm.root_id == root_id).all()



def get_indicator_by_name(db: Session, name: str, page_index: int, page_size: int):
    if name is None:
        name = ''
    name.lstrip()
    total = db.query(func.count(IndicatorOrm.indicator_id)).scalar()
    total_page = int((total + page_size - 1) / page_size)
    next_page = (page_index + 1)
    if next_page > total_page:
        next_page = None
    return {"data": db.query(IndicatorOrm).order_by(IndicatorOrm.indicator_id).filter(
        IndicatorOrm.name.like('%' + name + '%')).offset((page_index - 1) * page_size).limit(
        page_size).all(), "total": total, "page_index": page_index, "page_size": page_size, "total_page": total_page,
            "next_page": next_page}
    # return db.query(IndicatorOrm).filter(IndicatorOrm.name.like('%' + name + '%')).all()


def create_indicator(db: Session, ind: Indicator):

    orm_ind = IndicatorOrm(**ind.dict())
    db.add(orm_ind)
    db.commit()
    db.refresh(orm_ind)
    return orm_ind


def create_category(db: Session, ica: IndCategory):

    orm = IndCategoryOrm(**ica.dict())
    db.add(orm)
    db.commit()
    db.refresh(orm)
    return orm

def get_category_by_id(db:Session, c_id:str):
    return db.query(IndCategoryOrm).filter(IndCategoryOrm.c_id == c_id).first()



def get_category(db:Session, filter_str:str):
    return db.query(IndCategoryOrm).filter(IndCategoryOrm.c_id.like(filter_str)).all()
'''
