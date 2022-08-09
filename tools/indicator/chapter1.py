import csv
import Global_var
import datetime
from loguru import logger

indicator_id =''
unit = ''
ys = ['2019','2020','2021','2022']
def init_meta():
    
    f= open("./chapter1", mode="r", encoding="utf-8-sig")
    reader = csv.reader(f)
    header = next(reader)
    conn = Global_var.get_conn()
    cursor = conn.cursor()
    for row in reader:
        if len(row) == 0:
            continue
        _indicator_id = row[0]
        if _indicator_id.startswith('#'):
            break
        name = row[1]
        direction = row[2]
        unit = row[3]
        value = row[4]

        if value == '':
            continue
        if _indicator_id.startswith('2'):
            indicator_id = _indicator_id

            continue

        if _indicator_id.startswith('0'):
            print(row)
        for y in ys:
            sql = f"INSERT INTO ecrf.v3_indicator_meta (indicator_id, year, name, value, create_time, update_time, unit) VALUES ('{indicator_id}', '{y}', '{name}', '{value}', '{datetime.datetime.now()}', '{datetime.datetime.now()}', '{unit}');"
            #logger.info(sql)
            try:

                cursor.execute(sql)
                conn.commit()
            except Exception as err:
                logger.info(err)
                conn.rollback()






def generate_call():
    numerator=''
    f= open("./chapter1", mode="r", encoding="utf-8-sig")
    reader = csv.reader(f)
    header = next(reader)
    conn = Global_var.get_conn()
    cursor = conn.cursor()
    for row in reader:
        if len(row) == 0:
            continue
        _indicator_id = row[0]
        if _indicator_id.startswith('#'):
            break
        name = row[1]
        direction = row[2]
        _unit = row[3]
        value = row[4]

        if value == '':
            continue
        if _indicator_id.startswith('2'):
            indicator_id = _indicator_id
            unit = _unit
            continue

        if _indicator_id.startswith('0'):
            row
            #print(row)
        #for y in ys:
        if unit.find('百分比') >=0:
            if numerator =='':
                numerator = name
                continue
            
            #print(f" _process2_percent('{indicator_id}', db, '{numerator}', '{name}')")
            numerator = ''
        elif unit.find('比值') >=0:
            if numerator =='':
                numerator = name
                continue
            
            #print(f" _process2_ratio('{indicator_id}', db, '{numerator}', '{name}')")
            numerator = ''
        else:
            print(indicator_id)




if __name__ == '__main__':
    #init_meta()
    generate_call()