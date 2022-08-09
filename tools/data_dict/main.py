import csv
import sys
import psycopg2

sys.path.append('../')
import Global_var
import datetime
from loguru import logger
import xlrd


def pph():
    indicator_id = ''
    unit = ''
    ys = ['2019', '2020', '2021', '2022']

    data = xlrd.open_workbook('./PPH.xlsx')
    table = data.sheets()[0]
    nrows = table.nrows
    ls = []
    conn = Global_var.get_conn()
    cursor = conn.cursor()
    for l in range(nrows):
        if l == 0:
            continue
        code = table.row_values(l)[1]
        sql = f"update v3_operation_dict set ispph = '1'  where operation_code = '{code}'"
        cursor.execute(sql)
        logger.info(sql)
    conn.commit()
    cursor.close()
    conn.close()


def l4_surery():
    indicator_id = ''
    unit = ''
    ys = ['2019', '2020', '2021', '2022']

    data = xlrd.open_workbook('./Level_4_surgery.xlsx')
    table = data.sheets()[0]
    nrows = table.nrows
    ls = []
    conn = Global_var.get_conn()
    cursor = conn.cursor()
    for l in range(nrows):
        if l == 0:
            continue
        code = table.row_values(l)[1]
        name = table.row_values(l)[2]
        #sql = f"update v3_operation_dict set l4 = '1'  where operation_code = '{code}'"

        cursor.execute(
            "update v3_operation_dict set l4 = '1'  where operation_name = %s",
            [
                name,
            ])
        #logger.info(sql)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    #init_meta()
    l4_surery()
    print('.')
