import psycopg2
import time
import datetime
from loguru import logger


import math


# 内科科室 i
internal_medicine_department = [
    "儿童保健科病区",
    "放疗科病区",
    "风湿科病区",
    "感染科二病区",
    "呼吸与危重症医学科二病区",
    "呼吸与危重症医学科一病区",
    "精神卫生科病区",
    "康复医学科二病区",
    "康复医学科一病区",
    "内分泌科二病区",
    "内分泌科一病区",
    "皮肤科病区",
    "全科医疗科",
    "神经内科二病区",
    "神经内科三病区",
    "神经内科一病区",
    "肾内科二病区",
    "肾内科一病区",
    "消化内科二病区",
    "消化内科一病区",
    "小儿内科二病区",
    "小儿内科三病区",
    "小儿内科一病区",
    "心血管内科二病区",
    "心血管内科三病区",
    "心血管内科一病区",
    "血液内科二病区",
    "血液内科一病区",
    "中医科(肿瘤)病区",
    "中医科综合内科病区",
    "肿瘤内科二病区",
    "肿瘤内科一病区",
    "老年病科",
]
# 外科科室
survey_department = [
    "LDR产房",
    "产科二病区",
    "产科三病区",
    "产科一病区",
    "耳鼻咽喉科病区",
    "妇科二病区",
    "妇科一病区",
    "肛肠科病区",
    "骨外二科病区",
    "创伤中心",
    "骨外一科病区",
    "介入医学科",
    "口腔科病区",
    "泌尿外科一病区",
    "普外二科病区",
    "普外一科病区",
    "乳腺科病区",
    "烧伤整形外科病区",
    "神经外科二病区",
    "神经外科三病区",
    "神经外科一病区",
    "生殖医学病区",
    "手足外科病区",
    "疼痛科病区",
    "小儿外科病区",
    "心脏大血管外科病区",
    "胸外科二病区",
    "胸外科一病区",
    "血管外科",
    "眼科病区",
    "泌尿外科二病区",
]

conn = psycopg2.connect(
    database="linyidb",
    user="ecrf",
    password="ecrf2021.com",
    host="221.2.94.162",
    port="25432",
)


def get_conn():
    conn = None
    try:
        conn = psycopg2.connect(
            database="linyidb",
            user="ecrf",
            password="ecrf2021.com",
            host="221.2.94.162",
            port="25432",
        )
    except:
        logger.error("connect database failed")
        conn = None
    return conn


def calculate_month(datamonth, num):
    """
    月份加减函数,返回字符串类型
    :param datamonth: 时间(201501)
    :param num: 要加(减)的月份数量
    :return: 时间(str)
    """
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    datamonth = int(datamonth)
    num = int(num)
    year = datamonth // 100
    new_list = []
    s = math.ceil(abs(num) / 12)
    for i in range(int(-s), s + 1):
        new_list += [str(year + i) + x for x in months]
    new_list = [int(x) for x in new_list]
    return str(new_list[new_list.index(datamonth) + num])

