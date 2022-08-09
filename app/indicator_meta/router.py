# -*- coding: utf-8 -*-
#
# 模块路由文件
# Author: wonderful
# Email: 86531130@qq.com
# Created Time: 2022-05-17
# from typing import Dict
import json
import sys
# from fastapi import Depends, HTTPException
from typing import List
from typing import Optional

from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session

from .models import MetaOrm
from .schema import Meta
from .schema import MetaHole
from .schema import MetaHoleYearDept

from indicator_meta import crud
from indicator_meta import crud2

from dependencies import get_db
from collections import defaultdict

import xlrd

router = APIRouter(
    # dependencies=[Depends(get_token_header)],
    # responses={404: {"description": "Not found"}},
)


@router.get("/get_meta_by_id", summary='根据指标id获取相关meta数据', response_model=List)
async def get_meta_by_id(indicator_id: Optional[str] = Query(default=None, description="指标id"),
                         db: Session = Depends(get_db)):
    """根据指标id获取相关meta数据"""
    _indicator_id = indicator_id
    year2meta = defaultdict(list)
    _ms = crud.get_meta_by_indicator_id(db, indicator_id)
    if _indicator_id.startswith('2.2'):
        _ms = crud2.get_meta_by_indicator_id(db, indicator_id)

    ms = [{"year": '2019', "indicator_id": _indicator_id, "dataset": [], "size": 0},
          {"year": '2020', "indicator_id": _indicator_id, "dataset": [], "size": 0},
          {"year": '2021', "indicator_id": _indicator_id, "dataset": [], "size": 0},
          {"year": '2022', "indicator_id": _indicator_id, "dataset": [], "size": 0}]
    for _m in _ms:
        radix = int(_m.year) - 2019
        ms[radix]["dataset"].append({"id": _m.id, "unit": _m.unit, "value": _m.value,"name":_m.name,'year':_m.year})
        ms[radix]["size"] = len(ms[radix]["dataset"])
    print(ms)

    return ms  # json.dumps(ms)#crud.get_meta_by_indicator_id(db, indicator_id)


@router.get("/get_hole_by_id", summary='根据指标id获取相关meta下钻数据', response_model=List[MetaHole])
async def get_hole_by_id(indicator_id: Optional[str] = Query(default=None, description="指标id"),
                         db: Session = Depends(get_db)):
    """根据指标id获取相关meta下钻数据"""

    if indicator_id.startswith('2.2'):
        return crud2.get_meta_hole_by_indicator_id(db, indicator_id)

    return crud.get_meta_hole_by_indicator_id(db, indicator_id)


@router.get("/get_hole_year_dept_by_id", summary='根据指标id获取相关年度科室下钻数据', response_model=List[MetaHoleYearDept])
async def get_hole_year_dept_by_id(indicator_id: Optional[str] = Query(default='2.1.4.6', description="指标id"),year:str=Query(default='2021', description="年度"),
                         db: Session = Depends(get_db)):
    """根据指标id获取相关年度科室下钻数据"""
    if indicator_id.startswith('2.2'):
        return crud2.get_meta_year_dept_hole_by_indicator_id(db, indicator_id, year)

    return crud.get_meta_year_dept_hole_by_indicator_id(db, indicator_id,year)



@router.get("/get_hole_year_dept_by_meta_name", summary='据meta name获取相关年度科室下钻数据', response_model=List[MetaHoleYearDept])
async def get_hole_year_dept_by_meta_name(meta_name: Optional[str] = Query(default='医疗收入', description="meta_name"),year:str=Query(default='2021', description="年度"),
                         db: Session = Depends(get_db)):
    """根据meta name获取相关年度科室下钻数据"""

    return crud.get_meta_year_dept_hole_by_meta_name(db, meta_name,year)

@router.get("/get_hole_month_dept_by_meta_name", summary='据meta name获取相关月度科室下钻数据', response_model=List[MetaHoleYearDept])
async def get_hole_month_dept_by_meta_name(meta_name: Optional[str] = Query(default='年度出院患者次均医药费用', description="meta_name"),year:str=Query(default='2021', description="年度"),
                         db: Session = Depends(get_db)):
    """根据meta name获取相关月度科室下钻数据"""

    return crud.get_meta_month_dept_hole_by_meta_name(db, meta_name, year)







@router.put("/update_meta", summary='更新meta数据')
async def update_meta(ms: List[Meta], db: Session = Depends(get_db)):
    """更新meta数据"""
    crud.update(db, ms)
    return {"message": 0}


from .meta2indicator import process


@router.get("/meta2indicator", summary='根据meta数据计算指标')
async def meta2indicator(db: Session = Depends(get_db)):
    """更新meta数据"""
    process(db)


'''

@router.get("/get_ind_by_id", summary='根据id获取指标结果数据', response_model=Indicator)
async def ins(indicator_id: Optional[str] = Query(default=None, description="指标id"), db: Session = Depends(get_db)):
    """获取指标结果数据"""

    js_data = {
        "indicator_id": "2.1.1",
        "root_id": "第一章",
        "name": "核定床位数",
        "indicator_property": "定量指标",
        "unit": "张",
        "process_method": "无",
        "direction": "符合标准",
        "data_source": "数据",

    }
    ind = crud.get_indicator_by_id(db, indicator_id)
    # ind = IndicatorOrm(**js_data)

    return ind

@router.get("/get_category_by_id", summary='根据id获取分类数据', response_model=IndCategory)
async def ins(c_id: Optional[str] = Query(default=None, description="分类id"), db: Session = Depends(get_db)):
    """获取指标结果数据"""

    js_data = {
        "indicator_id": "2.1.1",
        "root_id": "第一章",
        "name": "核定床位数",
        "indicator_property": "定量指标",
        "unit": "张",
        "process_method": "无",
        "direction": "符合标准",
        "data_source": "数据",

    }
    ica = crud.get_category_by_id(db, c_id)
    # ind = IndicatorOrm(**js_data)

    return ica



@router.get("/get_ind_by_root_id", summary='根据root_id获取指标结果数据', response_model=List[Indicator])
async def ins(root_id: Optional[str] = Query(default=None, description="指标root_id"), db: Session = Depends(get_db)):
    """获取指标结果数据"""

    js_data = {
        "indicator_id": "2.1.1",
        "root_id": "第一章",
        "name": "核定床位数",
        "indicator_property": "定量指标",
        "unit": "张",
        "process_method": "无",
        "direction": "符合标准",
        "data_source": "数据",

    }
    ls = crud.get_indicator_by_root_id(db, root_id)
    # ind = IndicatorOrm(**js_data)

    return ls



@router.get("/get_ind_by_name", summary='根据name获取指标结果数据')
async def ins(name: Optional[str] = Query(default=None, description="指标名"), page_index: int = 1, page_size: int = 10,
              db: Session = Depends(get_db)):
    """获取指标结果数据"""

    ls = crud.get_indicator_by_name(db, name, page_index, page_size)
    # ind = IndicatorOrm(**js_data)

    return ls

'''
