# -*- coding: utf-8 -*-
#
# 模块路由配置文件
# Author: wonderful
# Email: 86531130@qq.com
# Created Time: 2022-05-17
from typing import Optional, Dict,List

from pydantic import BaseModel, Field


class Indicator(BaseModel):
    indicator_id: str = Field(..., title="指标编号")
    root_id: str = Field(..., title="章节")
    year: str = Field(default='2021', title="年度")
    name: str = Field(..., title="指标名称")
    indicator_property: str = Field(default='', title="指标属性")
    unit: str = Field(default='', title="计量单位")
    process_method: str = Field(default='', title="计算方法")
    direction: str = Field(default='', title="指标导向")
    data_source: str = Field(default='', title="数据来源")
    value: Optional[str] = Field(default='', title="指标值")
    js_value: List = Field(default={}, title="指标值")
    is_hole: int = Field(default=0, title="是否下钻")

    class Config:
        orm_mode = True


class IndCategory(BaseModel):
    c_id: str = Field(..., title='指标分类id')
    name: str = Field(default='', title="分类名称")

    class Config:
        orm_mode = True
