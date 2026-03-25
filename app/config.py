# -*- coding: utf-8 -*-
#
# 全局配置
# Author: wonderful
# Email: 86531130@qq.com
# Created Time: 2022-05-17

# 全局测试状态
#DEBUG = False
DEBUG = True

from typing import List
from pydantic import BaseSettings, validator, IPvAnyAddress, EmailStr, AnyHttpUrl


class Settings(BaseSettings):
    #
    #API_V1_STR: str = "/api/admin/v1"
    # SECRET_KEY 记得保密生产环境 不要直接写在代码里面
    #SECRET_KEY: str = "(-ASp+_)-Ulhw0848hnvVG-iqKyJSD&*&^-H3C9mqEqSl8KN-YRzRE"

    # token过期时间 60 minutes * 24 hours * 8 days = 8 days
   # ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8

    # 跨域设置 验证 list包含任意http url
    #BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = ['http://localhost']

    # 验证邮箱地址格式
    #FIRST_MALL: EmailStr = "wg_python@163.com"

    DATABASENAME:str = 'mirror'
    USERNAME:str = 'postgres'
    PASSWORD:str = 'j2UiYbIsDm3TZMP2'
    HOST:str = '222.194.63.79'
    PORT:str = '5432'

# 实例化配置对象
settings = Settings()
