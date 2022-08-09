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
        sql = f"update v3_indicator_meta_2 set value = '{v}' where name = '{name}' and year = '{y}';"
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

        sql = f"""update ecrf.v3_meta_hole_2 set value = '{json.dumps(vs)}' where  year = '{y}' and name = '{name}' """
        logger.info(sql)
        cursor.execute(sql)
        conn.commit()


def _process(pattern1, pattern2, name):
    _update_meta(pattern1, pattern2, name)
    #_hole(pattern1, pattern2, name)


def process():
    #第2章
    _process(
        """
            select count(distinct substr(disease_code, 0, 6) )
            from v3_emr_ich_diagnosis
            where sn = '1'
            and extract(year from discharge_time) = '{}' ;

        """,
        """
            select count(distinct substr(disease_code, 0, 6) )
            from v3_emr_ich_diagnosis
            where sn = '1'
            and extract(year from discharge_time) = '{}' and extract(month from discharge_time) = '{}';

        """,
        "收治病种数量（ICD-10亚目数量）",
    )
    _process(
        """
            select count(distinct substr(operation_code, 0, 6) )
            from v3_emr_ich_operation
            where
            extract(year from discharge_time) = '{}' ;

        """,
        """
            select count(distinct substr(operation_code, 0, 6) )
            from v3_emr_ich_operation
            where
            extract(year from discharge_time) = '{}'  and extract(month from discharge_time) = '{}';

        """,
        "住院术种数量（ICD-9-CM-3 细目数量）",
    )

    _process(
        """
            select count(distinct (czrk_id,inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code,' ') = t2.operation_code
            where t2.operation_type in ('0', '3') and  extract(year from discharge_time) = '{}';



        """,
        """
           select count(distinct (czrk_id,inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code,' ') = t2.operation_code
            where t2.operation_type in ('0', '3') and  extract(year from discharge_time) = '{}'

            and extract(month from discharge_date) = '{}';

        """,
        "同期出院患者手术台次数",
    )

    _process(
        """
            select count(*)
            from v3_emr_discharge_record
            where extract(year from discharge_date) = '{}';

        """,
        """
             select count(*)
            from v3_emr_discharge_record
            where extract(year from discharge_date) = '{}' and extract(month from discharge_date) = '{}';

        """,
        "同期出院患者总人次数",
    )

    _process(
        """
            select count(distinct (czrk_id,inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_name,' ') = t2.operation_name
            where t2.ispph = '1' and  extract(year from discharge_time) = '{}';

        """,
        """
            select count(distinct (czrk_id,inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_name,' ') = t2.operation_name
            where t2.ispph = '1' and  extract(year from discharge_time) = '{}' and extract(month from discharge_date) = '{}';

        """,
        "出院患者微创手术台次数",
    )

    _process(
        """
            select count(distinct (czrk_id,inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_name,' ') = t2.operation_name
            where t2.l4 = '1' and  extract(year from discharge_time) = '{}';

        """,
        """
            select count(distinct (czrk_id,inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_name,' ') = t2.operation_name
            where t2.l4 = '1' and  extract(year from discharge_time) = '{}' and extract(month from discharge_date) = '{}';

        """,
        "出院患者四级手术台次数",
    )

    _process(
        """
            select count(*)
            from v3_emr_discharge_record
            where discharge_situation = '5'
            and extract(year from discharge_date) = '{}';
        """,
        """
           select count(*)
            from v3_emr_discharge_record
            where discharge_situation = '5'
            and extract(year from discharge_date) = '{}' and extract(month from discharge_date) = '{}';
        """,
        "住院总死亡患者人数",
    )

    _process(
        """
                select count(*)
                from v3_emr_discharge_record
                where to_number(age_day, '999999999') < 29
                and discharge_situation = '5'
                and extract(year from discharge_date) = '{}';


            """,
        """
                select count(*)
                from v3_emr_discharge_record
                where to_number(age_day, '999999999') < 29
                and discharge_situation = '5'
                and extract(year from discharge_date) = '{}' and extract(month from discharge_date) = '{}';
            """,
        "新生儿患者住院死亡人数",
    )

    _process(
        """
                select count(*)
                from v3_emr_discharge_record
                where to_number(age_day, '999999999') < 29
                and extract(year from discharge_date) = '{}';


            """,
        """
                select count(*)
                from v3_emr_discharge_record
                where to_number(age_day, '999999999') < 29
                and extract(year from discharge_date) = '{}' and extract(month from discharge_date) = '{}';
            """,
        "同期新生儿患者出院人次",
    )

    _process(
        """
            select count(distinct (t1.czrk_id,t1.inpatient_num))
            from v3_emr_ich_operation t1
                left join v3_emr_discharge_record t2
                on t1.czrk_id = t2.czrk_id and t1.inpatient_num = t2.inpatient_num
                    left join v3_operation_dict t3 on btrim(t1.operation_code,' ') = t3.operation_code
            where t3.operation_type in ('0', '3')
              and t2.discharge_situation = '5'
              and  extract(year from t2.discharge_date) = '{}';


            """,
        """
            select count(distinct (t1.czrk_id,t1.inpatient_num))
            from v3_emr_ich_operation t1
                left join v3_emr_discharge_record t2
                on t1.czrk_id = t2.czrk_id and t1.inpatient_num = t2.inpatient_num
                    left join v3_operation_dict t3 on btrim(t1.operation_code,' ') = t3.operation_code
            where t3.operation_type in ('0', '3')
              and t2.discharge_situation = '5'
              and  extract(year from t2.discharge_date) = '{}' and extract(month from t2.discharge_date) = '{}';

            """,
        "手术患者住院死亡人数",
    )

    _process(
        """
               select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) = 'I26.9'
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);



            """,
        """

                 select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) = 'I26.9'
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);

            """,
        "手术患者手术后肺栓塞发生例数",
    )

    _process(
        """
               select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) in ('I80.2','I82.8')
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);



            """,
        """

                 select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) in ('I80.2','I82.8')
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);

            """,
        "手术患者手术后深静脉血栓发生例数",
    )

    _process(
        """
               select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where (substr(disease_code, 0, 6) in
                   ('A40.0', 'A40.1', 'A40.2', 'A40.3', 'A40.8', 'A40.9', 'A41.0', 'A41.1', 'A41.2', 'A41.3', 'A41.4',
                    'A41.5', 'A41.8', 'A41.9') or substr(disease_code, 0, 7)  = 'T81.411')
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);



            """,
        """

                 select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where (substr(disease_code, 0, 6) in
                   ('A40.0', 'A40.1', 'A40.2', 'A40.3', 'A40.8', 'A40.9', 'A41.0', 'A41.1', 'A41.2', 'A41.3', 'A41.4',
                    'A41.5', 'A41.8', 'A41.9') or substr(disease_code, 0, 7)  = 'T81.411')
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);

            """,
        "手术患者手术后败血症发生例数",
    )

    _process(
        """
               select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) = 'T81.0'
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);



            """,
        """

                 select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) = 'T81.0'
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);

            """,
        "手术患者手术后出血或血肿发生例数",
    )
    _process(
        """
               select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) = 'T81.3'
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);



            """,
        """

                 select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) = 'T81.3'
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);

            """,
        "手术患者手术伤口裂开发生例数",
    )
    _process(
        """
               select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) in ('R96.0','R96.1','I46.1')
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);



            """,
        """

                 select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) in ('R96.0','R96.1','I46.1')
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);

            """,
        "手术患者手术后猝死发生例数",
    )
    _process(
        """
               select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) in  ('J96.0','J96.1','J96.9')
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);



            """,
        """

                 select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) in  ('J96.0','J96.1','J96.9')
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);

            """,
        "手术患者手术后呼吸衰竭发生例数",
    )
    _process(
        """
               select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) in  ('E89.0','E89.1','E89.2','E89.3','E89.4','E89.5','E89.6','E89.8','E89.9')
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);



            """,
        """

                 select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) in  ('E89.0','E89.1','E89.2','E89.3','E89.4','E89.5','E89.6','E89.8','E89.9')
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);

            """,
        "手术患者手术后生理/代谢紊乱发生例数",
    )
    _process(
        """
               select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) = 'T81.4'
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);



            """,
        """

                 select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '3')
                and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) = 'T81.4'
               and (admission_condition = '4' or admission_condition is null)
               and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}' and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);

            """,
        "与手术/操作相关感染发生例数",
    )