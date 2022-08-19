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
                where t2.operation_type in ('0', '1', '2', '3')
                and extract(year from discharge_time) = '{0}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) = 'T81.4'
               and (admission_condition = '4' or admission_condition is null)
               and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);



            """,
        """

                 select count(distinct (czrk_id,inpatient_num))
                from v3_emr_ich_operation t1
                        left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
                where t2.operation_type in ('0', '1', '2', '3')
                and extract(year from discharge_time) = '{0}' and extract(month from discharge_time) = '{1}'
                and exists(select *
                from v3_emr_ich_diagnosis t3
                where substr(disease_code, 0, 6) = 'T81.4'
               and (admission_condition = '4' or admission_condition is null)
               and t1.czrk_id = t3.czrk_id  and t1.inpatient_num = t3.inpatient_num);

            """,
        "与手术/操作相关感染发生例数",
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '1', '2', '3')
            and extract(year from t1.discharge_time) = '{0}';
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '1', '2', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}';
        """,
        "同期出院患者手术/操作人次数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('T81.5', 'T81.6')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('T81.5', 'T81.6')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "发生手术过程中异物遗留的出院发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('T88.2', 'T88.3', 'T88.4', 'T88.5')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('T88.2', 'T88.3', 'T88.4', 'T88.5')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "手术患者麻醉并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('J95.1', 'J95.2', 'J95.3', 'J95.4', 'J95.5', 'J95.8', 'J95.9', 'J98.4')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('J95.1', 'J95.2', 'J95.3', 'J95.4', 'J95.5', 'J95.8', 'J95.9', 'J98.4')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "手术患者肺部感染与肺机能不全发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) = 'T81.2'
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) = 'T81.2'
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "发生手术意外穿刺伤或撕裂伤的发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('N17.0', 'N17.1', 'N17.2', 'N17.3', 'N17.4', 'N17.5', 'N17.6', 'N17.7', 'N17.8', 'N17.9', 'N99.0')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('N17.0', 'N17.1', 'N17.2', 'N17.3', 'N17.4', 'N17.5', 'N17.6', 'N17.7', 'N17.8', 'N17.9', 'N99.0')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "手术后急性肾衰竭发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('K91.0', 'K91.1', 'K91.2', 'K91.3', 'K91.4', 'K91.5', 'K91.8', 'K91.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('K91.0', 'K91.1', 'K91.2', 'K91.3', 'K91.4', 'K91.5', 'K91.8', 'K91.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "消化系统术后并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('I97.0', 'I97.1', 'I97.8', 'I97.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('I97.0', 'I97.1', 'I97.8', 'I97.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "循环系统术后并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where (substr(t3.disease_code, 0, 6) in ('G97.0', 'G97.1', 'G97.8', 'G97.9') or substr(t3.disease_code, 0, 4) in ('I60', 'I61', 'I62', 'I63', 'I64'))
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where (substr(t3.disease_code, 0, 6) in ('G97.0', 'G97.1', 'G97.8', 'G97.9') or substr(t3.disease_code, 0, 4) in ('I60', 'I61', 'I62', 'I63', 'I64'))
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "神经系统术后并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('H59.0', 'H59.8', 'H59.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('H59.0', 'H59.8', 'H59.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "眼和附器术后并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('H95.0', 'H95.1', 'H95.8', 'H95.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('H95.0', 'H95.1', 'H95.8', 'H95.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "耳和乳突术后并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('M96.0', 'M96.1', 'M96.2', 'M96.3', 'M96.4', 'M96.5', 'M96.6', 'M96.8', 'M96.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('M96.0', 'M96.1', 'M96.2', 'M96.3', 'M96.4', 'M96.5', 'M96.6', 'M96.8', 'M96.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "肌肉骨骼术后并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('N99.0', 'N99.1', 'N99.2', 'N99.3', 'N99.4', 'N99.5', 'N99.8', 'N99.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('N99.0', 'N99.1', 'N99.2', 'N99.3', 'N99.4', 'N99.5', 'N99.8', 'N99.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "泌尿生殖系统术后并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('K11.4', 'S04.3', 'S04.5')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('K11.4', 'S04.3', 'S04.5')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "口腔术后并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in 
                            ('T82.0', 'T82.1', 'T82.2', 'T82.3', 'T82.4', 'T82.5', 'T82.6', 'T82.7', 'T82.8', 'T82.9',
                            'T83.0', 'T83.1', 'T83.2', 'T83.3', 'T83.4', 'T83.5', 'T83.6', 'T83.8', 'T83.9',
                            'T84.0', 'T84.1', 'T84.2', 'T84.3', 'T84.4', 'T84.5', 'T84.6', 'T84.7', 'T84.8', 'T84.9',
                            'T85.0', 'T85.1', 'T85.2', 'T85.3', 'T85.4', 'T85.5', 'T85.6', 'T85.7', 'T85.8', 'T85.9'
                            )
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in
                            ('T82.0', 'T82.1', 'T82.2', 'T82.3', 'T82.4', 'T82.5', 'T82.6', 'T82.7', 'T82.8', 'T82.9',
                            'T83.0', 'T83.1', 'T83.2', 'T83.3', 'T83.4', 'T83.5', 'T83.6', 'T83.8', 'T83.9',
                            'T84.0', 'T84.1', 'T84.2', 'T84.3', 'T84.4', 'T84.5', 'T84.6', 'T84.7', 'T84.8', 'T84.9',
                            'T85.0', 'T85.1', 'T85.2', 'T85.3', 'T85.4', 'T85.5', 'T85.6', 'T85.7', 'T85.8', 'T85.9'
                            )
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "植入物的并发症（不包括脓毒症）发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('T86.0', 'T86.1', 'T86.2', 'T86.3', 'T86.4', 'T86.8', 'T86.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('T86.0', 'T86.1', 'T86.2', 'T86.3', 'T86.4', 'T86.8', 'T86.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "移植的并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('T87.0', 'T87.1', 'T87.2', 'T87.3', 'T87.4', 'T87.5', 'T87.6')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('T87.0', 'T87.1', 'T87.2', 'T87.3', 'T87.4', 'T87.5', 'T87.6')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "再植和截肢的并发症发生例数"
    )
    _process(
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('T81.1', 'T81.7', 'T81.8', 'T81.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(distinct (t1.czrk_id, t1.inpatient_num))
            from v3_emr_ich_operation t1
                    left join v3_operation_dict t2 on btrim(t1.operation_code, ' ') = t2.operation_code
            where t2.operation_type in ('0', '3')
            and extract(year from t1.discharge_time) = '{0}'
            and extract(month from t1.discharge_time) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t3
                        where substr(t3.disease_code, 0, 6) in ('T81.1', 'T81.7', 'T81.8', 'T81.9')
                        and (t3.admission_condition = '4' or t3.admission_condition is null)
                        and t3.czrk_id = t1.czrk_id
                        and t3.inpatient_num = t1.inpatient_num);
        """,
        "介入操作与手术后患者其他并发症发生例数"
    )
    _process(
        """
            select count(*)
            from v3_emr_discharge_record t1
            where to_number(t1.age_day, '999999999') <= 28
            and extract(year from t1.discharge_date) = '{0}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t2
                        where substr(t2.disease_code, 0, 6) in
                            ('P10.0', 'P10.1', 'P10.2', 'P10.3', 'P10.4', 'P10.8', 'P10.9',
                            'P11.0', 'P11.1', 'P11.2', 'P11.3', 'P11.4', 'P11.5', 'P11.9',
                            'P12.0', 'P12.1', 'P12.2', 'P12.3', 'P12.4', 'P12.8', 'P12.9',
                            'P13.0', 'P13.1', 'P13.2', 'P13.3', 'P13.4', 'P13.8', 'P13.9',
                            'P14.0', 'P14.1', 'P14.2', 'P14.3', 'P14.8', 'P14.9',
                            'P15.0', 'P15.1', 'P15.2', 'P15.3', 'P15.4', 'P15.5', 'P15.6', 'P15.8', 'P15.9',
                            'P20.0', 'P20.1', 'P20.9',
                            'P21.0', 'P21.1', 'P21.9',
                            'P22.0', 'P22.1', 'P22.8', 'P22.9'
                            )
                        and (t2.admission_condition = '4' or t2.admission_condition is null)
                        and t2.czrk_id = t1.czrk_id
                        and t2.inpatient_num = t1.inpatient_num);
        """,
        """
            select count(*)
            from v3_emr_discharge_record t1
            where to_number(t1.age_day, '999999999') <= 28
            and extract(year from t1.discharge_date) = '{0}'
            and extract(month from t1.discharge_date) = '{1}'
            and exists(select 1
                        from v3_emr_ich_diagnosis t2
                        where substr(t2.disease_code, 0, 6) in
                            ('P10.0', 'P10.1', 'P10.2', 'P10.3', 'P10.4', 'P10.8', 'P10.9',
                            'P11.0', 'P11.1', 'P11.2', 'P11.3', 'P11.4', 'P11.5', 'P11.9',
                            'P12.0', 'P12.1', 'P12.2', 'P12.3', 'P12.4', 'P12.8', 'P12.9',
                            'P13.0', 'P13.1', 'P13.2', 'P13.3', 'P13.4', 'P13.8', 'P13.9',
                            'P14.0', 'P14.1', 'P14.2', 'P14.3', 'P14.8', 'P14.9',
                            'P15.0', 'P15.1', 'P15.2', 'P15.3', 'P15.4', 'P15.5', 'P15.6', 'P15.8', 'P15.9',
                            'P20.0', 'P20.1', 'P20.9',
                            'P21.0', 'P21.1', 'P21.9',
                            'P22.0', 'P22.1', 'P22.8', 'P22.9'
                            )
                        and (t2.admission_condition = '4' or t2.admission_condition is null)
                        and t2.czrk_id = t1.czrk_id
                        and t2.inpatient_num = t1.inpatient_num);
        """,
        "新生儿发生产伤的例数",
    )
    _process(
        """
            select count(*)
            from v3_emr_discharge_record
            where to_number(age_day, '999999999') <= 28
            and extract(year from discharge_date) = '{0}';
        """,
        """
            select count(*)
            from v3_emr_discharge_record
            where to_number(age_day, '999999999') <= 28
            and extract(year from discharge_date) = '{0}'
            and extract(month from discharge_date) = '{1}';
        """,
        "同期新生儿出院患者总人次数",
    )