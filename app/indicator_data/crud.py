from sqlalchemy import func
from sqlalchemy.orm import Session

from .models import IndicatorOrm
from .schema import Indicator
from .models import IndCategoryOrm
from .schema import IndCategory


def get_indicator_by_id(db: Session, indicator_id: str):
    return db.query(IndicatorOrm).filter(IndicatorOrm.indicator_id == indicator_id).first()

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
