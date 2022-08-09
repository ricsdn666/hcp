import Global_var
from loguru import logger
from datetime import datetime
from datetime import timedelta
import threading
import json
from collections import defaultdict


ys = ["2019", "2020", "2021", "2022"]
ms = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]


def _meta_hole_year_dept(pattern1, pattern2, name):
    conn = Global_var.get_conn()
    cursor = conn.cursor()
    for y in ys:
        sql_year = pattern1.format(y)
        logger.info(sql_year)
        cursor.execute(sql_year)
        rs = cursor.fetchall()
        vs_dept2v = []
        for r in rs:
            dep_name = r[0]
            
            if r[1] is  None:
                continue

            v = round(r[1], 2)
            vs_dept2v.append({"dept_name": dep_name, "value": str(v)})
        sql = f"update v3_meta_hole_year_dept set value = '{json.dumps(vs_dept2v,ensure_ascii=False)}' where name = '{name}' and year = '{y}' ; "
        logger.info(sql)
        cursor.execute(sql)
    conn.commit()


def _meta_hole_month_dept(pattern1, pattern2, name):
    conn = Global_var.get_conn()
    cursor = conn.cursor()

    for y in ys:
        dept2value = {}
        sql_m = pattern2.format(y)
        logger.info(sql_m)
        cursor.execute(sql_m)
        rs= cursor.fetchall()
        #v_12 = ['' for _ in range(12)]
        for r in rs:
            dept_name = r[0]
            rdx = int(r[1])
            if r[2] is None:
                continue
            v = round(r[2], 2)
            if dept_name not in dept2value:
                dept2value[dept_name] = ['' for _ in range(12)]
            
            dept2value[dept_name][rdx-1] = str(v)
            #v_12[rdx-1] = str(v)
        ls_dept_value = [{"dept_name":k,"value":v} for k,v in dept2value.items()]
        #ls_dept_value.append({"dept_name": r[0], "value": v_12})
        sql = f"""update ecrf.v3_meta_hole_month_dept set value = '{json.dumps(ls_dept_value,ensure_ascii=False)}' where  year = '{y}' and name = '{name}' """
        logger.info(sql)
        cursor.execute(sql)
        conn.commit()


def _process(pattern1, pattern2, name):
    #_meta_hole_year_dept(pattern1, pattern2, name)
     _meta_hole_month_dept(pattern1, pattern2, name)


def process():
    '''
    _process(
        "select dep_name,avg(total_amount) v from v3_emr_discharge_record t1 where extract(year from t1.discharge_date) = '{}' group by dep_name order by v desc;",
        """
            select dep_name, extract(month from t1.discharge_date),avg(total_amount)
            from v3_emr_discharge_record t1
            where extract(year from t1.discharge_date) = '{}'
            group by dep_name,extract(month from t1.discharge_date);
        """,
        "年度出院患者次均医药费用",
    )

    _process(
        "select treatment_section_name,avg(total_amount) v from v3_emr_outpatient_record t1 where extract(year from date(t1.treatment_date)) = '{}' group by treatment_section_name order by v desc;",
        """
            select treatment_section_name, extract(month from date(t1.treatment_date)),avg(total_amount)
            from v3_emr_outpatient_record t1
            where extract(year from date(t1.treatment_date)) = '{}'
            group by treatment_section_name,extract(month from date(t1.treatment_date));
        """,
        
        "年度门诊患者次均医药费用",
    )

    _process(
        "select ORD_DEP_NAME,sum(proj_mon) v from v3_emr_in_fee_info_child t1 where extract(year from t1.rcd_dt) = '{}' and charge_type not in ('1','2','3','6','7') group by ORD_DEP_NAME order by v desc;",
        """
            select ORD_DEP_NAME, extract(month from t1.rcd_dt),sum(proj_mon)
            from v3_emr_in_fee_info_child t1
            where extract(year from t1.rcd_dt) = '{}'
            and charge_type not in ('1', '2', '3', '6', '7')
            group by ORD_DEP_NAME , extract(month from t1.rcd_dt);

        """,
        "医疗服务收入",
    )
    
    _process(
        "select ORD_DEP_NAME,sum(proj_mon) v from v3_emr_in_fee_info_child t1 where extract(year from t1.rcd_dt) = '{}' group by ORD_DEP_NAME order by v desc;",
        """
                select ORD_DEP_NAME, extract(month from t1.rcd_dt), sum(proj_mon)
                from v3_emr_in_fee_info_child t1
                where extract(year from t1.rcd_dt) = '{}'
                group by ORD_DEP_NAME, extract(month from t1.rcd_dt);
        """,
        
        "住院收入",
    )
    
    _process(
        """ 
            select ord_dep_name, sum(t3.fee) v
            from (
                    (select ord_dep_name, sum(proj_mon) fee
                    from v3_emr_in_fee_info_child t1
                    where extract(year from t1.rcd_dt) = '{0}'
                    GROUP by t1.ord_dep_name
                    )
                    union all
                    (
                        select ord_dep_name, sum(proj_mon) fee
                        from v3_emr_out_fee_info_child t2
                        where extract(year from t2.rcd_dt) = '{0}'
                        GROUP by t2.ord_dep_name)) t3
            group by t3.ord_dep_name
            order by v desc;
        """,
        """
            select ord_dep_name, m, sum(t3.fee) v
            from (
                    (select ord_dep_name, extract(month from t1.rcd_dt) m, sum(proj_mon) fee
                    from v3_emr_in_fee_info_child t1
                    where extract(year from t1.rcd_dt) = '{0}'
                    GROUP by t1.ord_dep_name, extract(month from t1.rcd_dt)
                    )
                    union all
                    (
                        select ord_dep_name, extract(month from t2.rcd_dt) m, sum(proj_mon) fee
                        from v3_emr_out_fee_info_child t2
                        where extract(year from t2.rcd_dt) = '{0}'
                        GROUP by t2.ord_dep_name, extract(month from t2.rcd_dt))
                ) t3
            group by t3.ord_dep_name, m;
                    
        
        ;""",
        "医疗收入",
    )

    '''
    _process(
        """
        select t1.dep_name,sum(case (date(t1.discharge_date) - date(t1.admission_date))
               when 0 then 1
               else date(t1.discharge_date) - date(t1.admission_date) end)
        from v3_emr_discharge_record t1
        where extract(year from discharge_date) = '{}' group by  t1.dep_name;
        """,
        """
            select t1.dep_name,extract(month from discharge_date),sum(case (date(t1.discharge_date) - date(t1.admission_date))
               when 0 then 1
               else date(t1.discharge_date) - date(t1.admission_date) end)
            from v3_emr_discharge_record t1
            where extract(year from discharge_date) = '{}' group by  t1.dep_name,extract(month from discharge_date);

        """,
        
        "实际占用的总床日数",
    )

