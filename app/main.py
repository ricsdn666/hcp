# -*- coding: utf-8 -*-
#
# 全局入口文件
# Author: wonderful
# Email: 86531130@qq.com
# Created Time: 2022-05-17
from fastapi import FastAPI

from scheduler import  scheduler
from fastapi.openapi.docs import (
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.staticfiles import StaticFiles
# from fastapi import Depends
from fastapi.middleware.cors import CORSMiddleware

from settings import DEBUG
from utils import parse_readme
from schema import VersionResp
import uvicorn

version = "0.5.0"  # 系统版本号
title, description = parse_readme()
app = FastAPI(
    debug=DEBUG,
    title=title,
    description=description,
    version=version,
    docs_url=None,  # 关闭原有的文档地址
    #openapi_tags=['公共接口'],
    # dependencies=[Depends(get_query_token),
)


def register_scheduler(app: FastAPI):
    @app.on_event("startup")
    async def start_event():
        print('定时任务未启动')
        #scheduler.start()
        #print("定时任务启动成功")

#register_scheduler(app)

# 跨域问题

origins = ['*']
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# *****************************************************
# 解决接口文档的静态文件问题
# *****************************************************
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - 接口文档",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="/static/swagger-ui-bundle.js",
        swagger_css_url="/static/swagger-ui.css",
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


# *****************************************************


# redis连接
# from common.connections import init_redis
# init_redis('192.168.1.242')   # 配置redis host

# 加载模块路由
# from test_module.router import router as test_router
# app.include_router(test_router, prefix="/test", tags=["测试模块"])

from indicator_data.router import router as target_router
from indicator_meta.router import router as meta_router

app.include_router(target_router, prefix='/target', tags=['指标结果数据'])
app.include_router(meta_router, prefix='/meta', tags=['指标meta数据'])

# 加载验证码模块
# from captcha_module.router import router as captcha_router
# app.include_router(captcha_router, prefix="/captcha", tags=["验证码模块"])


@app.get("/version", summary='获取系统版本号',
         response_model=VersionResp)
async def version_api():
    """获取系统版本号"""
    return {"version": version}




from loguru import logger
import time
if __name__ == '__main__':
    t = time.strftime("%Y_%m_%d")
    logger.add(f"./log/{t}.log", rotation="500MB", encoding="utf-8", enqueue=True, compression="zip")
    # logger.add("/Users/wonderful/code/dss/dss_timed_jobs/log/tes.log")
    logger.info("app start")
    uvicorn.run(app='main:app', host='0.0.0.0', port=8888, reload=True, debug=True, workers=1)
