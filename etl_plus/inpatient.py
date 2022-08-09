import Global_var
from loguru import  logger
from datetime import  datetime
from datetime import timedelta
import threading
#住院费用明细归集到patient

def _fee2patient(year):
    conn = Global_var.get_conn()
    cusor = conn.cursor()
    start = datetime.strptime(f'{year}-1-1', "%Y-%m-%d")
    end = datetime.strptime(f'{int(year)+1}-1-1', "%Y-%m-%d")
    while(start < end):
        #cur = conn.cursor()
        sql_patient = f"""select czrk_id,inpatient_num from v3_emr_discharge_record where date(discharge_date)  = date('{start.strftime("%Y-%m-%d")}')"""
        logger.info(sql_patient)
        cusor.execute(sql_patient)
        rs = cusor.fetchall()

        for r in rs:
            #logger.info(r[0],r[1])
            sql_all_cost = f"""update v3_emr_discharge_record t1
    set total_amount = (select sum(t2.proj_mon)
                              from v3_emr_in_fee_info_child t2
                              where t2.czrk_id = '{r[0]}'
                                and t2.inpatient_num = '{r[1]}') where t1.czrk_id = '{r[0]}' and inpatient_num = '{r[1]}'
                                """
            logger.info(sql_all_cost)
            cusor.execute(sql_all_cost)
        conn.commit()
        start = start + timedelta(days=1)
        #break
    conn.close()
    


ys = ['2019','2020','2021','2022']
def fee2patient():
    ts = []
    for y in ys:
        t = threading.Thread(target=_fee2patient,args=(y,))
        ts.append(t)
        t.start()
    for t in ts:
        t.join()
        
