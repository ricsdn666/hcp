# -*- coding: utf-8 -*-
#
# 模块路由配置文件
# Author: wonderful
# Email: 86531130@qq.com
# Created Time: 2022-05-17
from typing import Optional, Dict,List

from pydantic import BaseModel, Field


class Meta(BaseModel):
    id: int = Field(..., title="主键")
    indicator_id : str = Field(title="指标编号")
    year: str = Field(default='2021', title="年度")
    name: str = Field(..., title="指标相关数据")
    value: Optional[str] = Field(default='', title="值")
    unit: str= Field(..., title= "单位")

    class Config:
        orm_mode = True


class MetaHole(BaseModel):
    id: int = Field(..., title="主键")
    indicator_id : str = Field(title="指标编号")
    year: str = Field(default='2021', title="年度")
    name: str = Field(..., title="指标相关数据")
    value: List = Field(default={}, title="指标值")
    unit: str= Field(..., title= "单位")

    class Config:
        orm_mode = True



class MetaHoleYearDept(BaseModel):
    id: int = Field(..., title="主键")
    indicator_id : str = Field(title="指标编号")
    year: str = Field(default='2021', title="年度")
    name: str = Field(..., title="指标相关数据")
    value: List = Field(default={}, title="指标值")
    unit: str= Field(..., title= "单位")

    class Config:
        orm_mode = True


class MetaHoleMonthDept(BaseModel):
    id: int = Field(..., title="主键")
    indicator_id : str = Field(title="指标编号")
    year: str = Field(default='2021', title="年度")
    name: str = Field(..., title="指标相关数据")
    value: List = Field(default={}, title="指标值")
    unit: str= Field(..., title= "单位")

    class Config:
        orm_mode = True

