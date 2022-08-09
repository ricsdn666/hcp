import json

from .crud import get_meta_by_name

ys = ['2019', '2020', '2021', '2022']


def process(db):
    '''
    第一章
    '''

    _process('2.1.1.1', db)
    _process('2.1.1.2', db)

    _process2_percent('2.1.1.3', db, '实际占用的总床日数', '实际开放的总床日数')
    _process2_ratio('2.1.2.1', db, '医院卫生技术人员数', '同期医院实际开放床位数')
    _process2_ratio('2.1.2.2', db, '医院执业护士总人数', '同期医院实际开放床位数')
    _process2_ratio('2.1.2.3', db, '医院病区执业护士总人数', '同期医院实际开放床位数')
    _process2_ratio('2.1.2.4', db, '医院感染管理专职人员数', '同期医院实际开放床位数')
    _process2_percent('2.1.3.1.1', db, '急诊医学科固定在岗（本院）医师人数', '同期急诊医学科在岗医师人数')
    _process2_percent('2.1.3.1.2', db, '急诊医学科固定执业护士人数', '同期急诊医学科在岗执业 护士人数')
    _process2_percent('2.1.3.2.1', db, '重症医学科开放床位数', '同期医院实际开放床位数')
    _process2_ratio('2.1.3.2.2', db, '重症医学科医师人数', '同期重症医学科实际开放床位数')
    _process2_ratio('2.1.3.2.3', db, '重症医学科执业护士人数', '同期重症医学科实际开放床位数')
    _process2_ratio('2.1.3.3.1', db, '麻醉科固定在岗（本院）医师总数', '同期实施麻醉的手术间数 ')
    _process2_ratio('2.1.3.3.2', db, '麻醉科固定在岗（本院）医师总数', '同期麻醉科日均全麻手术例数 ')
    _process2_percent('2.1.3.4.1', db, '中医科实际开放床位数', '同期医院实际开放床位数')
    _process2_ratio('2.1.3.4.2', db, '中医科中医类别医师人数', '同期中医科实际开放床位数')
    _process2_ratio('2.1.3.4.3', db, '中医科执业护士人数', '同期中医科实际开放床位数')
    _process2_percent('2.1.3.5.1', db, '康复医学科实际开放床位数', '同期医院实际开放床位数')
    _process2_ratio('2.1.3.5.2', db, '康复医学科医师人数', '同期康复医学科实际开放床位数')
    _process2_ratio('2.1.3.5.3', db, '康复医学科康复师人数', '同期康复医学科实际开放床位数')
    _process2_ratio('2.1.3.5.4', db, '康复医学科执业护士人数', '同期康复医学科实际开放床位数')
    _process2_percent('2.1.3.6.1', db, '感染性疾病科固定医师人数', '同期感染性疾病科在岗医师人数')
    _process2_percent('2.1.3.6.2', db, '感染性疾病科固定执业护士人数', '同期感染性疾病科所有在岗执业护士人数')
    _process2_percent('2.1.3.6.3', db, '感染性疾病科实际开放床位数', '同期医院实际开放床位数')
    _process2_percent('2.1.3.6.4', db, '可转换感染性疾病床位数', '同期医院实际开放床位数')
    _process2_percent('2.1.4.1', db, '住院收入', '医疗收入')
    _process2_percent('2.1.4.2', db, '医疗服务收入', '医疗收入')
    _process2_percent('2.1.4.3', db, '人员经费', '医疗活动费用')

    _process2_1_4_4('2.1.4.4', db, '年总能耗', '年总收入')

    # _process2_percent('2.1.4.5', db, '（本年度门诊患者次均医药费用－上一年度门诊患者次均医药费用）', '上一年度门诊患者次均医药费用')
    # _process2_percent('2.1.4.6', db, '（本年度出院患者次均医药费用－上一年度出院患者次均医药费用）', '上一年度出院患者次均医药费用')
    process_increase_ratio('2.1.4.5', db, '年度门诊患者次均医药费用')
    process_increase_ratio('2.1.4.6', db, '年度出院患者次均医药费用')

    _process2_percent('2.1.5.1', db, '本年度科研项目立项经费总金额', '同期医院卫生技术人员总数')

    _process('2.1.5.2', db)

    '''
    第二章
    '''

    process('2.2.1.1', '收治病种数量（ICD-10亚目数量）')
    process('2.2.1.2', '住院术种数量（ICD-9-CM-3 细目数量）')
    _process2_percent('2.2.1.7', db, '同期出院患者手术台次数', '同期出院患者人次数')
    _process2_percent('2.2.1.8', db, '出院患者微创手术台次数', '同期出院患者手术台次数')
    _process2_percent('2.2.1.9', db, '出院患者四级手术台次数', '同期出院患者手术台次数')
    _process2_percent('2.2.2.2', db, '住院总死亡患者人数', '同期出院患者总人次数')
    _process2_percent('2.2.2.3', db, '新生儿患者住院死亡人数', '同期出院患者总人次数')
    _process2_percent('2.2.2.4', db, '手术患者住院死亡人数', '同期出院患者手术人次数')

    _process2_permillage('2.2.3.1', '手术患者手术后肺栓塞发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.2', '手术患者手术后深静脉血栓发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.3', '手术患者手术后败血症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.4', '手术患者手术后出血或血肿发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.5', '手术患者手术伤口裂开发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.6', '手术患者手术后猝死发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.7', '手术患者手术后呼吸衰竭发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.8', '手术患者手术后生理/代谢紊乱发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.9', '与手术/操作相关感染发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.10', '发生手术过程中异物遗留的出院发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.11', '手术患者麻醉并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.12', '手术患者肺部感染与肺机能不全发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.13', '发生手术意外穿刺伤或撕裂伤的发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.14', '手术后急性肾衰竭发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.15.1', '消化系统术后并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.15.2', '循环系统术后并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.15.3', '神经系统术后并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.15.4', '眼和附器术后并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.15.5', '耳和乳突术后并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.15.6', '肌肉骨骼术后并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.15.7', '泌尿生殖系统术后并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.15.8', '口腔术后并发症发生例数', '同期出院患者手术人次数')

    _process2_permillage('2.2.3.16', '植入物的并发症（不包括脓毒症）发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.17', '移植的并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.18', '再植和截肢的并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.19', '介入操作与手术后患者其他并发症发生例数', '同期出院患者手术人次数')
    _process2_permillage('2.2.3.20', '新生儿发生产伤的例数', '同期新生儿出院患者总人次数')
    _process2_permillage('2.2.3.21', '阴道分娩产妇产程和分娩并发症发生例数', '同期出院阴道分娩总人次数')
    _process2_permillage('2.2.3.22', '剖宫产分娩产妇产程和分娩并发症发生例数', '同期出院剖宫产分娩总人次数')
    _process2_permillage('2.2.3.23', '住院患者 2 期及以上院内压力性损伤新发病例数', '同期出院剖宫产分娩总人次数')
    _process2_permillage('2.2.3.24', '输注反应的发生例数', '同期出院患者总人次数')
    _process2_permillage('2.2.3.25', '输血反应的发生例数', '同期输血出院患者总人次数')
    _process2_permillage('2.2.3.26', '医源性气胸的发生例数', '同期出院患者总人次')
    _process2_permillage('2.2.3.27', '住院患者医院内发生跌倒/坠床所致髋部骨折发生例数', '同期出院患者总人次')
    _process2_permillage('2.2.3.27', '住院患者医院内发生跌倒/坠床所致髋部骨折发生例数', '同期出院患者总人次')
    _process2_permillage('2.2.3.28', 'ICU住院患者呼吸机相关性肺炎发生例数', '同期住院ICU患者总人次')
    _process2_permillage('2.2.3.29', 'ICU住院患者血管导管相关性感染发生例数', '同期住院ICU患者总人次')
    _process2_permillage('2.2.3.30', 'ICU患者导尿管相关性尿路感染发生例数', '同期住院ICU患者总人次')
    _process2_permillage('2.2.3.31', '临床用药所致的有害效应（不良事件）发生例数', '同期出院患者总人次')
    _process2_permillage('2.2.3.32', '血液透析所致并发症发生例数', '同期血液透析出院患者总人次数')


def _process(indicator_id, db):
    # indicator_id = '2.1.1.1'
    js_value = []
    sql_meta = f"select year,value from v3_indicator_meta where indicator_id = '{indicator_id}' order by year asc;";
    cursor = db.execute(sql_meta)
    rs = cursor.fetchall()
    for r in rs:
        js_value.append({"year": r[0], "value": r[1]})

    sql_indicator = f"update v3_indicator set js_value = '{json.dumps(js_value)}' where  indicator_id = '{indicator_id}'"
    db.execute(sql_indicator)
    db.commit()


def _process2_percent(indicator_id, db, numerator, denominator):
    js_value = []
    for y in ys:
        m1 = get_meta_by_name(db, numerator, y)
        m2 = get_meta_by_name(db, denominator, y)
        js_value.append({"year": y, "value": f"{round(float(m1.value) * 100 / float(m2.value), 2)}"})
    sql_indicator = f"update v3_indicator set js_value = '{json.dumps(js_value)}' where  indicator_id = '{indicator_id}'"
    db.execute(sql_indicator)
    db.commit()


def _process2_permillage(indicator_id, db, numerator, denominator):
    js_value = []
    for y in ys:
        m1 = get_meta_by_name(db, numerator, y)
        m2 = get_meta_by_name(db, denominator, y)
        js_value.append({"year": y, "value": f"{round(float(m1.value) * 1000 / float(m2.value), 2)}"})
    sql_indicator = f"update v3_indicator set js_value = '{json.dumps(js_value)}' where  indicator_id = '{indicator_id}'"
    db.execute(sql_indicator)
    db.commit()


def _process2_ratio(indicator_id, db, numerator, denominator):
    js_value = []
    for y in ys:
        m1 = get_meta_by_name(db, numerator, y)
        m2 = get_meta_by_name(db, denominator, y)
        js_value.append({"year": y, "value": f"{round(float(m1.value) / float(m2.value), 2)}:1"})
    sql_indicator = f"update v3_indicator set js_value = '{json.dumps(js_value)}' where  indicator_id = '{indicator_id}'"
    db.execute(sql_indicator)
    db.commit()


def _process2_1_4_4(indicator_id, db, numerator, denominator):
    js_value = []
    for y in ys:
        m1 = get_meta_by_name(db, numerator, y)
        m2 = get_meta_by_name(db, denominator, y)
        js_value.append({"year": y, "value": f"{round(float(m1.value) / (float(m2.value) / 10000), 2)}"})
    sql_indicator = f"update v3_indicator set js_value = '{json.dumps(js_value)}' where  indicator_id = '{indicator_id}'"
    db.execute(sql_indicator)
    db.commit()


def _process2_1_5_1(indicator_id, db, numerator, denominator):
    js_value = []
    for y in ys:
        m1 = get_meta_by_name(db, numerator, y)
        m2 = get_meta_by_name(db, denominator, y)
        js_value.append({"year": y, "value": f"{round(float(m1.value) / (float(m2.value / 100)), 2)}"})
    sql_indicator = f"update v3_indicator set js_value = '{json.dumps(js_value)}' where  indicator_id = '{indicator_id}'"
    db.execute(sql_indicator)
    db.commit()


import copy


def process_increase_ratio(indicator_id, db, numerator):
    js_value = []
    _m = None
    for y in ys:
        m = get_meta_by_name(db, numerator, y)
        if _m is None:
            _m = copy.deepcopy(m)
            js_value.append({"year": y, "value": '0'})
            continue

        js_value.append(
            {"year": y, "value": f"{round(100 * (float(m.value) - float(_m.value)) / (float(_m.value)), 2)}"})

        _m = copy.deepcopy(m)
    sql_indicator = f"update v3_indicator set js_value = '{json.dumps(js_value)}' where  indicator_id = '{indicator_id}'"
    db.execute(sql_indicator)
    db.commit()
