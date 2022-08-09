import Global_var
from loguru import logger
from datetime import datetime
from datetime import timedelta
import threading
import json


ys = ["2019", "2020", "2021", "2022"]
ms = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]


def _update_meta(pattern1, pattern2, name):
    conn = Global_var.get_conn()
    cursor = conn.cursor()
    for y in ys:
        sql_year = pattern1.format(y)
        logger.info(sql_year)
        cursor.execute(sql_year)
        r = cursor.fetchone()
        v = round(r[0], 2)
        sql = f"update v3_indicator_meta set value = '{v}' where name = '{name}' and year = '{y}';"
        logger.info(sql)
        cursor.execute(sql)
        conn.commit()


def _hole(pattern1, pattern2, name):
    conn = Global_var.get_conn()
    cursor = conn.cursor()
    
    for y in ys:
        vs = []
        for m in ms:
            sql_m = pattern2.format(y, m)
            logger.info(sql_m)
            cursor.execute(sql_m)
            r = cursor.fetchone()
            v = 0
            if r[0] is not None:
                v = round(r[0], 2)
            vs.append(str(v))

        sql = f"""update ecrf.v3_meta_hole set value = '{json.dumps(vs)}' where  year = '{y}' and name = '{name}' """
        logger.info(sql)
        cursor.execute(sql)
        conn.commit()


def _process(pattern1, pattern2, name):
    _update_meta(pattern1, pattern2, name)
    _hole(pattern1, pattern2, name)


def process():
#第一章
    _process(
        "select avg(total_amount) from v3_emr_discharge_record t1 where extract(year from t1.discharge_date) = '{}';",
        "select avg(total_amount) from v3_emr_discharge_record t1 where extract(year from t1.discharge_date) = '{}' and extract(month from t1.discharge_date) = '{}' ;",
        "年度出院患者次均医药费用",
    )
    _process(
         "select avg(total_amount) from v3_emr_outpatient_record t1 where extract(year from date(t1.treatment_date)) = '{}';",
         "select avg(total_amount) from v3_emr_outpatient_record t1 where extract(year from date(t1.treatment_date)) = '{}' and extract(month from date(t1.treatment_date)) = '{}';",
         '年度门诊患者次均医药费用'
    )

    _process(
        "select sum(proj_mon) from v3_emr_in_fee_info_child t1 where extract(year from t1.rcd_dt) = '{}' and charge_type not in ('1','2','3','6','7');",
        "select sum(proj_mon) from v3_emr_in_fee_info_child t1 where extract(year from t1.rcd_dt) = '{}' and extract(month from t1.rcd_dt) = '{}' and charge_type not in ('1','2','3','6','7');",
        "医疗服务收入",
    )

    _process(
        "select sum(proj_mon) from v3_emr_in_fee_info_child t1 where extract(year from t1.rcd_dt) = '{}';",
        "select sum(proj_mon) from v3_emr_in_fee_info_child t1 where extract(year from t1.rcd_dt) = '{}' and extract(month from t1.rcd_dt) = '{}' ;",
        "住院收入",
    )

    _process(
        """select sum(_in + _out)  from
        (select sum(proj_mon) _in  from v3_emr_in_fee_info_child t1 where  extract(year from t1.rcd_dt) = '{0}') t3
        ,
        (select  sum(proj_mon) _out from v3_emr_out_fee_info_child  t2 where   extract(year from t2.rcd_dt) = '{0}') t4""",
        """select sum(_in + _out)  from
        (select sum(proj_mon) _in  from v3_emr_in_fee_info_child t1 where  extract(year from t1.rcd_dt) = '{0}' and extract(month from t1.rcd_dt) = '{1}') t3
        ,
        (select  sum(proj_mon) _out from v3_emr_out_fee_info_child  t2 where   extract(year from t2.rcd_dt) = '{0}' and extract(month from t2.rcd_dt) = '{1}') t4;""",
        "医疗收入",
    )


    _process(
        """
        select sum(case (date(t1.discharge_date) - date(t1.admission_date))
               when 0 then 1
               else date(t1.discharge_date) - date(t1.admission_date) end)
            from v3_emr_discharge_record t1
            where extract(year from discharge_date) = '{}';
        """,
        
        """
         select sum(case (date(t1.discharge_date) - date(t1.admission_date))
               when 0 then 1
               else date(t1.discharge_date) - date(t1.admission_date) end)
            from v3_emr_discharge_record t1
            where extract(year from discharge_date) = '{}' and extract(month from discharge_date) = '{}';
        """
        ,
        "实际占用的总床日数",
    )

