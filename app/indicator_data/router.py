# -*- coding: utf-8 -*-
#
# 模块路由文件
# Author: wonderful
# Email: 86531130@qq.com
# Created Time: 2022-05-17
# from typing import Dict
# from fastapi import Depends, HTTPException
from typing import List
from typing import Optional

import xlrd
from common.connections import SessionLocal
from fastapi import APIRouter, Query, Depends
from indicator_data import crud
from sqlalchemy.orm import Session

from .schema import IndCategory
from .schema import Indicator

router = APIRouter(
    # dependencies=[Depends(get_token_header)],
    # responses={404: {"description": "Not found"}},
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/", summary='模块测试API')
async def test_api():
    """模块测试API"""
    return {'message': 'ok'}


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

    def comp2(x, y):
        ind1 = x.indicator_id.split('.')
        ind2 = y.indicator_id.split('.')
        if int(ind1[2]) > int(ind2[2]):
            return 1
        elif int(ind1[2]) < int(ind2[2]):
            return -1
        else:

            if int(ind1[3]) > int(ind2[3]):
                return 1
            elif int(ind1[3]) < int(ind2[3]):
                return -1

        return 0

    def comp3(x, y):
        ind1 = x.indicator_id.split('.')
        ind2 = y.indicator_id.split('.')
        if int(ind1[3]) > int(ind2[3]):
            return 1
        elif int(ind1[3]) < int(ind2[3]):
            return -1
        else:

            if int(ind1[4]) > int(ind2[4]):
                return 1
            elif int(ind1[4]) < int(ind2[4]):
                return -1

        return 0



    import functools
    #_ls = sorted(ls,key=comp)
    if root_id == '2.2.1':
        ls.sort(key=functools.cmp_to_key(comp2))

    if root_id == '2.2.2':
        ls.sort(key=functools.cmp_to_key(comp3))
    return ls


@router.get("/get_ind_by_name", summary='根据name获取指标结果数据')
async def ins(name: Optional[str] = Query(default=None, description="指标名"), page_index: int = 1, page_size: int = 10,
              db: Session = Depends(get_db)):
    """获取指标结果数据"""

    ls = crud.get_indicator_by_name(db, name, page_index, page_size)
    # ind = IndicatorOrm(**js_data)

    return ls


@router.get("/create_all", summary='创建所有指标', response_model=List[Indicator])
async def ins(db: Session = Depends(get_db)):
    """创建所有指标数据"""

    db.execute('delete from indicator')
    db.execute('delete from ind_category')
    data = xlrd.open_workbook('/Users/wonderful/code/third-level-review/app/indicator_data/data.xls')
    table = data.sheets()[0]
    nrows = table.nrows
    ls = []
    for l in range(nrows):
        if l == 0:
            continue

        indicator_id = table.row_values(l)[0]
        if indicator_id.split('.')[0] != '2':
            continue
        name = table.row_values(l)[1]
        direction = table.row_values(l)[2]
        unit = table.row_values(l)[3]
        value = table.row_values(l)[4]
        if value == '':
            _ica = {
                "cid"
            }
            ica = IndCategory(c_id=indicator_id, name=name)
            crud.create_category(db, ica)
            continue
        year = '2021'
        root_id = ".".join(indicator_id.split('.')[:-1])
        print(root_id)
        _data = {
            "root_id": root_id,
            "indicator_id": indicator_id,
            "name": name,
            "direction": direction,
            "unit": unit,
            "value": value
        }
        ind = Indicator(
            **_data
        )
        ls.append(crud.create_indicator(db, ind))
    return ls
    '''
    ls = crud.get_category(db, '2.1._')
    js = {"2.1":[]}
    for l in ls:
        ica = IndCategory.from_orm(l)
        if ica.c_id == '2.1':
            js[ica] = []
        print(ica)
    '''


def bg_task():
    print("bg_task")
