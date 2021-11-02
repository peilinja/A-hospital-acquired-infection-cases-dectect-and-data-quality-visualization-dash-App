import sys
import traceback
import dash_uploader as du
import dash
from dash import html
import plotly.express as px
from dash import dcc
from dash.dependencies import Output, Input, State
import uuid
import time
import zipfile
from pydantic import BaseModel
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from joblib import Parallel, delayed
import re
from flask import send_from_directory
import dash_bootstrap_components as dbc

class Param(BaseModel):
    dbname: str
    dbdriver: str
    dbhost: str
    dbport: str
    dbuser: str
    dbpasswd: str
    dborcl: str
    cbasics: str
    antis: str
    opers: str
    temps: str
    bars: str
    departments: str
    allantinames: str
    begintime: str
    endtime: str
    process: str
    hospname: str



TEMPLATE = "plotly_white"

app = dash.Dash(
    __name__,
    external_stylesheets=['https://cdn.staticfile.org/twitter-bootstrap/4.5.2/css/bootstrap.min.css'],
    suppress_callback_exceptions=True,
    routes_pathname_prefix="/dash/"
)
app.title = "nis感染性事件计算"

server = app.server

upload_path = os.getcwd() + '\\upload'
du.configure_upload(app, upload_path, use_upload_id=True)



def list_months(btime, etime):
    import calendar
    def get_current_month_start_and_end(date):
        """
        年份 date(2017-09-08格式)
        :param date:
        :return:本月第一天日期和本月最后一天日期
        """
        if date.count('-') != 2:
            raise ValueError('- is error')
        year, month = str(date).split('-')[0], str(date).split('-')[1]
        end = calendar.monthrange(int(year), int(month))[1]
        start_date = '%s-%s-01 00:00:01' % (year, month)
        end_date = '%s-%s-%s 23:59:59' % (year, month, end)
        return start_date, end_date

    mon_lis = list(pd.date_range(btime, etime, freq='M').astype(str))
    mon_lis[0] = btime
    if mon_lis[-1][0:7] < etime[0:7]:
        mon_lis.append(etime)
    bt = []
    et = []
    for i in mon_lis:
        mon_s_e = get_current_month_start_and_end(i)
        if i == btime:
            bt.append(btime + ' 00:00:01')
            et.append(mon_s_e[1])
        elif i == etime:
            bt.append(mon_s_e[0])
            et.append(etime + ' 23:59:59')
        else:
            bt.append(mon_s_e[0])
            et.append(mon_s_e[1])

    return bt, et

def discriminated_antis(all_antis):
    try:
        df_抗菌药物 = pd.read_csv(r'../抗菌药物字典.csv')
    except:
        df_抗菌药物 = pd.read_csv(r'../抗菌药物字典.csv', encoding='gbk')
    def isanti(x):
        df_抗菌药物['药品'] = x.抗菌药物
        df1 = df_抗菌药物[df_抗菌药物['规则等级']==1]
        if x.抗菌药物 in list(df1['匹配规则'].values):
            return df1[df1['匹配规则']==x.抗菌药物].reset_index(drop=True).loc[0]['抗菌药物通用名']
        else:
            df2 = df_抗菌药物[df_抗菌药物['规则等级']==2]
            df2['是否匹配'] = df2.apply(lambda y: y.抗菌药物通用名 if re.match(y.匹配规则, y.药品) else np.nan, axis=1)
            df2['匹配长度'] = df2.apply(lambda y: 0 if pd.isnull(y.是否匹配) else len( y.匹配规则 ), axis=1)
            if df2[~df2['是否匹配'].isnull()].shape[0]==0:
                df3 = df_抗菌药物[df_抗菌药物['规则等级']==3]
                df3['是否匹配'] = df3.apply(lambda y: y.抗菌药物通用名 if re.match(y.匹配规则, y.药品) else np.nan, axis=1)
                df3['匹配长度'] = df3.apply(lambda y: 0 if pd.isnull(y.是否匹配) else len( y.匹配规则 ), axis=1)
                if df3[~df3['是否匹配'].isnull()].shape[0]==0:
                    df4 = df_抗菌药物[df_抗菌药物['规则等级']==4]
                    df4['是否匹配'] = df4.apply(lambda y: y.抗菌药物通用名 if re.match(y.匹配规则, y.药品) else np.nan, axis=1)
                    df4['匹配长度'] = df4.apply(lambda y: 0 if pd.isnull(y.是否匹配) else len( y.匹配规则 ), axis=1)
                    if df4[~df4['是否匹配'].isnull()].shape[0]==0:
                        return np.nan
                    else:
                        return df4[~df4['是否匹配'].isnull()][['抗菌药物通用名','匹配长度']].drop_duplicates().sort_values(by=['匹配长度'], ascending=False).reset_index(drop=True)['抗菌药物通用名'].loc[0]#返回正则匹配成功且匹配长度最长
                else:
                    return df3[~df3['是否匹配'].isnull()][['抗菌药物通用名','匹配长度']].drop_duplicates().sort_values(by=['匹配长度'], ascending=False).reset_index(drop=True)['抗菌药物通用名'].loc[0]#返回正则匹配成功且匹配长度最长
            else:
                return df2[~df2['是否匹配'].isnull()][['抗菌药物通用名','匹配长度']].drop_duplicates().sort_values(by=['匹配长度'], ascending=False).reset_index(drop=True)['抗菌药物通用名'].loc[0]#返回正则匹配成功且匹配长度最长
    all_antis['抗菌药物通用名'] = all_antis.apply(isanti, axis=1)
    return all_antis

def get_upload_component(id):
    uid = uuid.uuid1()
    print(uid)
    return du.Upload(
        id=id,
        max_files=1000,
        text='拖放或者点击这里进行上传！',
        text_completed='上传完成: ',
        cancel_button=True,
        pause_button=True,
        max_file_size=1800,  # 1800 Mb
        # filetypes=['csv', 'zip'],
        filetypes=['csv'],
        upload_id=uid,  # Unique session id
)

def bg_compute(btime, etime, param, antis_dict):
    print("开始执行%s-%s,进程号为%d,开始时间为%s" % ( btime, etime, os.getpid(), pd.datetime.now()))
    time1 = time.time()
    engine = create_engine(
        param.dbname + "+" + param.dbdriver + "://" + param.dbuser + ":" + param.dbpasswd + "@" + param.dbhost + ":" + param.dbport + "/" + param.dborcl,
        echo=False,
        encoding='UTF-8')
    min_time = pd.to_datetime('1680-01-01')
    now_time = datetime.now()

    try:
        antis_权重 = pd.read_csv(r'../抗菌药物权重.csv')
    except:
        antis_权重 = pd.read_csv(r'../抗菌药物权重.csv', encoding='gbk')

    antis_权重 = antis_权重[['抗菌药物通用名', '权重']]

    # 术前高等级用药和术后48小时高等级用药字典
    oper_高等级抗菌药物1 = pd.DataFrame(
        ['比阿培南', '厄他培南', '美罗培南', '帕尼培南倍他米隆', '法罗培南', '亚胺培南西司他丁', '头孢曲松钠舒巴坦', '头孢曲松钠他唑巴坦', '头孢噻肟舒巴坦', '头孢哌酮钠舒巴坦',
         '头孢哌酮他唑巴坦',
         '头孢他啶他唑巴坦', '依替米星', '异帕米星', '多西环素', '米诺环素', '替加环素', '替考拉宁', '万古霉素', '去甲万古霉素', '利奈唑胺', '泊沙康唑', '阿莫罗芬', '布替萘芬',
         '大蒜素', '伏康唑', '氟胞嘧啶', '氟康唑',
         '卡泊芬净', '克霉唑', '利福霉素', '联苯苄唑', '两性霉素B', '咪康唑', '米卡芬净', '那他霉素', '曲安奈德益康唑', '特比萘芬', '酮康唑', '伊曲康唑'],
        columns=['高等级抗菌药物1'])
    oper_高等级抗菌药物1['是否高等级用药1'] = 1
    oper_高等级抗菌药物1 = oper_高等级抗菌药物1.set_index(['高等级抗菌药物1'])

    oper_高等级抗菌药物2 = pd.DataFrame(
        ['比阿培南', '厄他培南', '美罗培南', '帕尼培南倍他米隆', '法罗培南', '亚胺培南西司他丁', '多西环素', '米诺环素', '替加环素', '替考拉宁', '万古霉素', '去甲万古霉素',
         '利奈唑胺',
         '泊沙康唑', '阿莫罗芬', '布替萘芬', '大蒜素', '伏康唑', '氟胞嘧啶', '氟康唑', '卡泊芬净', '克霉唑', '利福霉素', '联苯苄唑', '两性霉素B', '咪康唑', '米卡芬净',
         '那他霉素',
         '曲安奈德益康唑', '特比萘芬', '酮康唑', '伊曲康唑'], columns=['高等级抗菌药物2'])
    oper_高等级抗菌药物2['是否高等级用药2'] = 1
    oper_高等级抗菌药物2 = oper_高等级抗菌药物2.set_index(['高等级抗菌药物2'])


    # 统计时间段数据读取
    cbasic = pd.read_sql_query(param.cbasics, params=(btime, etime), con=engine)

    departments = pd.read_sql_query(param.departments, params=(btime, etime), con=engine)

    antis = pd.read_sql(param.antis, params=(btime, etime), con=engine)

    opers = pd.read_sql(param.opers, params=(btime, etime), con=engine)

    bars = pd.read_sql(param.bars, params=(btime, etime), con=engine)

    temps = pd.read_sql(param.temps, params=(btime, etime), con=engine)


    # 医疗业务数据信息情况统计
    df_antis = antis[['caseid']].drop_duplicates()
    df_antis['是否存在抗菌药物医嘱信息'] = '是'

    df_departments = departments[['caseid']].drop_duplicates()
    df_departments['是否存在adt信息'] = '是'

    df_opers = opers[['caseid']].drop_duplicates()
    df_opers['是否存在手术信息'] = '是'

    df_bars = bars[['caseid']].drop_duplicates()
    df_bars['是否存在送检信息'] = '是'

    df_temps = temps[['caseid']].drop_duplicates()
    df_temps['是否存在体征信息'] = '是'
    lis_菌 = ['caseid','大肠埃希菌', '鲍曼不动杆菌', '肺炎克雷伯菌', '金黄色葡萄球菌', '铜绿假单胞菌', '屎肠球菌', '粪肠球菌']
    # 多耐药君检出次数统计
    df_多重耐药菌 = bars[bars.apply(lambda x: x.菌检出 in ['大肠埃希菌', '鲍曼不动杆菌', '肺炎克雷伯菌', '金黄色葡萄球菌', '铜绿假单胞菌', '屎肠球菌', '粪肠球菌'],
                               axis=1)].drop_duplicates().reset_index(drop=True)
    if df_多重耐药菌.shape[0] > 0:
        df_多重耐药菌 = df_多重耐药菌.groupby(["菌检出", "caseid"]).count()
        df_多重耐药菌.rename(columns={'检验申请时间': '检出次数'}, inplace=True)
        df_多重耐药菌 = df_多重耐药菌.reset_index()
        df_多重耐药菌 = df_多重耐药菌.set_index(["菌检出", "caseid"])["检出次数"]
        df_多重耐药菌 = df_多重耐药菌.unstack(level=0)
        df_多重耐药菌 = df_多重耐药菌.rename_axis(columns=None)
        df_多重耐药菌 = df_多重耐药菌.reset_index()
        if len(df_多重耐药菌.columns)<8:
            df_多重耐药菌1 = pd.DataFrame(columns=lis_菌)
            for idx_bar in lis_菌:
                df_多重耐药菌1[idx_bar] = df_多重耐药菌[idx_bar] if idx_bar in df_多重耐药菌.columns else np.nan
            df_多重耐药菌 = df_多重耐药菌1

    else:
        df_多重耐药菌 = pd.DataFrame(columns=['caseid', '大肠埃希菌', '屎肠球菌', '粪肠球菌', '肺炎克雷伯菌', '金黄色葡萄球菌', '铜绿假单胞菌', '鲍曼不动杆菌'])
    df_多重耐药菌.rename(
        columns={'大肠埃希菌': '大肠埃希菌检出次数', '鲍曼不动杆菌': '鲍曼不动杆菌检出次数', '肺炎克雷伯菌': '肺炎克雷伯菌检出次数', '金黄色葡萄球菌': '金黄色葡萄球菌检出次数',
                 '铜绿假单胞菌': '铜绿假单胞菌检出次数', '屎肠球菌': '屎肠球菌检出次数', '粪肠球菌': '粪肠球菌检出次数'}, inplace=True)

    # 患者基本信息，由于表输入时以入院时间作为条件，所以不对入院时间做校验，只校验出院时间
    # 校验规则： 出院时间小于入院时间或者出院时间大于当前时间为问题数据
    cbasic['是否出院患者'] = np.where(cbasic['出院时间'].isnull(), '否', '是')
    cbasic['出院时间'] = np.where((cbasic['出院时间'].isnull()) | (cbasic['出院时间'] == '9999'), str(now_time)[0:19],cbasic['出院时间'])
    cbasic['出院时间是否存在问题'] = np.where((cbasic['出院时间'] < btime) | (cbasic['出院时间'] > str(now_time)[0:19]), '是', '否')

    df_出院时间存在问题 = cbasic[(cbasic['出院时间'] < btime) | (cbasic['出院时间'] > str(now_time)[0:19])]
    df_出院时间存在问题['医院名称'] = param.hospname
    df_医院名称 = param.hospname
    # 通过基本信息关联患者入出转、给药、手术、体温、菌检出数据情况表，并将空置为’否‘
    df_感染性事件数据质量表 = cbasic.merge(df_departments, on='caseid', how='left')
    df_感染性事件数据质量表['医院名称'] = param.hospname
    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(df_antis, on='caseid', how='left')
    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(df_bars, on='caseid', how='left')
    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(df_opers, on='caseid', how='left')
    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(df_temps, on='caseid', how='left')
    df_感染性事件数据质量表 = df_感染性事件数据质量表.fillna('否')

    # 关联菌检出、并将空置为0
    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(df_多重耐药菌, on='caseid', how='left')
    df_感染性事件数据质量表 = df_感染性事件数据质量表.fillna(0)

    # 患者adt信息预处理 ： 将出科时间为空和出科时间为“9999”替换为了当前时间
    # 校验规则： 出科时间为小于入院时间或者大于出院时间；入科时间为空、入科时间小于入院时间、入科时间大于出科时间、入科时间大于出院时间

    departments = departments.merge(cbasic[['caseid', '入院时间', '出院时间']], on='caseid')
    departments['出科时间'] = np.where((departments['出科时间'].isnull()) | (departments['出科时间'] == '9999'),
                                   departments['出院时间'], departments['出科时间'])

    departments['出入科时间存在问题次数'] = np.where((departments['出科时间'] < departments['入院时间']) |
                                          (departments['出科时间'] > departments['出院时间']) |
                                          (departments['入科时间'].isnull()) |
                                          (departments['入科时间'] < departments['入院时间']) |
                                          (departments['入科时间'] > departments['出科时间']) |
                                          (departments['入科时间'] > departments['出院时间']), 1, 0)

    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(departments.groupby(['caseid'])[['出入科时间存在问题次数']].sum().reset_index(),on='caseid', how='left')

    # 患者抗菌药物医嘱信息预处理 ： 将医嘱结束时间为空和“9999”替换为了出院时间
    # 校验规则： 医嘱结束时间为小于入院时间或者大于出院时间；医嘱开始时间为空、医嘱开始时间小于入院时间、医嘱开始时间大于医嘱结束时间、医嘱开始时间大于出院时间

    antis = antis.merge(cbasic[['caseid', '入院时间', '出院时间']], on='caseid')
    antis['医嘱结束时间'] = np.where((antis['医嘱结束时间'].isnull()) | (antis['医嘱结束时间'] == '9999'), antis['出院时间'], antis['医嘱结束时间'])
    antis['医嘱开始结束时间存在问题次数'] = np.where((antis['医嘱结束时间'] < antis['入院时间']) |
                                       (antis['医嘱结束时间'] > antis['出院时间']) |
                                       (antis['医嘱开始时间'] < antis['入院时间']) |
                                       (antis['医嘱开始时间'] > antis['医嘱结束时间']) |
                                       (antis['医嘱开始时间'].isnull()) |
                                       (antis['医嘱开始时间'] > antis['出院时间']), 1, 0)

    df_医嘱开始结束时间存在问题 = antis[(antis['医嘱结束时间'] < antis['入院时间']) |
                            (antis['医嘱结束时间'] > antis['出院时间']) |
                            (antis['医嘱开始时间'] < antis['入院时间']) |
                            (antis['医嘱开始时间'] > antis['医嘱结束时间']) |
                            (antis['医嘱开始时间'].isnull()) |
                            (antis['医嘱开始时间'] > antis['出院时间'])]

    df_医嘱开始结束时间存在问题['医院名称'] = param.hospname


    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(antis.groupby(['caseid'])[['医嘱开始结束时间存在问题次数']].sum().reset_index(), on='caseid',how='left')

    # 患者手术信息预处理 ： 将手术结束时间为空和手术结束时间为“9999”替换为了出院时间
    # 校验规则： '手术结束时间为小于入院时间或者大于出院时间；'手术开始时间为空、'手术开始时间小于入院时间、'手术开始时间大于'手术结束时间、'手术开始时间大于出院时间

    opers = opers.merge(cbasic[['caseid', '入院时间', '出院时间']], on='caseid')
    opers['手术结束时间'] = np.where((opers['手术结束时间'].isnull()) | (opers['手术结束时间'] == '9999'), opers['出院时间'], opers['手术结束时间'])
    opers['手术开始结束时间存在问题次数'] = np.where((opers['手术结束时间'] < opers['入院时间']) |
                                       (opers['手术结束时间'] > opers['出院时间']) |
                                       (opers['手术开始时间'] < opers['入院时间']) |
                                       (opers['手术开始时间'] > opers['手术结束时间']) |
                                       (opers['手术开始时间'].isnull()) |
                                       (opers['手术开始时间'] > opers['出院时间']), 1, 0)

    df_手术开始结束时间存在问题 = opers[(opers['手术结束时间'] < opers['入院时间']) |
                            (opers['手术结束时间'] > opers['出院时间']) |
                            (opers['手术开始时间'] < opers['入院时间']) |
                            (opers['手术开始时间'] > opers['手术结束时间']) |
                            (opers['手术开始时间'].isnull()) |
                            (opers['手术开始时间'] > opers['出院时间'])]
    df_手术开始结束时间存在问题['医院名称'] = param.hospname


    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(opers.groupby(['caseid'])[['手术开始结束时间存在问题次数']].sum().reset_index(), on='caseid',how='left')

    # 患者体温记录信息预处理 ： 无
    # 校验规则： '测量时间为空、'测量时间小于入院时间、测量时间大于出院时间

    temps = temps.merge(cbasic[['caseid', '入院时间', '出院时间']], on='caseid')
    temps['入院时间_1'] = temps['入院时间'].map(lambda x: x[0:10])
    temps['出院时间_1'] = temps['出院时间'].map(lambda x: x[0:10])
    temps['测量时间_1'] = temps['测量时间'].map(lambda x: x[0:10])
    temps['体温信息测量时间存在问题次数'] = np.where( (temps['测量时间'].isnull()) | (temps['测量时间_1'] > temps['出院时间_1']) | (temps['测量时间_1'] < temps['入院时间_1']), 1, 0)

    df_体温信息测量时间存在问题 = temps[ (temps['测量时间'].isnull()) | (temps['测量时间_1'] > temps['出院时间_1']) | (temps['测量时间_1'] < temps['入院时间_1'])]
    df_体温信息测量时间存在问题['医院名称'] = param.hospname

    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(temps.groupby(['caseid'])[['体温信息测量时间存在问题次数']].sum().reset_index(), on='caseid', how='left')

    # 患者微生物送检信息预处理 ： 无
    # 校验规则： '检验申请时间为空、'检验申请时间小于入院时间、检验申请时间大于出院时间

    bars = bars.merge(cbasic[['caseid', '入院时间', '出院时间']], on='caseid')
    bars['微生物检验申请时间存在问题次数'] = np.where( (bars['检验申请时间'].isnull()) | (bars['检验申请时间'] > bars['出院时间']) | (bars['检验申请时间'] < bars['入院时间']), 1, 0)

    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(bars.groupby(['caseid'])[['微生物检验申请时间存在问题次数']].sum().reset_index(), on='caseid', how='left')

    # 纳入计算患者数据
    datas = df_感染性事件数据质量表[((df_感染性事件数据质量表['出院时间是否存在问题'] == '否') &
                           ((df_感染性事件数据质量表['医嘱开始结束时间存在问题次数'] == 0) | pd.isnull(df_感染性事件数据质量表['医嘱开始结束时间存在问题次数']))
                           & ((df_感染性事件数据质量表['手术开始结束时间存在问题次数'] == 0) | pd.isnull(df_感染性事件数据质量表['手术开始结束时间存在问题次数']))
                           & ((df_感染性事件数据质量表['体温信息测量时间存在问题次数'] == 0) | pd.isnull(df_感染性事件数据质量表['体温信息测量时间存在问题次数'])))]
    datas = datas[['caseid', '入院时间', '出院时间', '入院科室', '年龄', '是否出院患者']]

    antis = antis[antis['caseid'].isin(datas['caseid'].drop_duplicates())]
    bars = bars[bars['caseid'].isin(datas['caseid'].drop_duplicates())]
    temps = temps[temps['caseid'].isin(datas['caseid'].drop_duplicates())]
    opers = opers[opers['caseid'].isin(datas['caseid'].drop_duplicates())]
    departments = departments[departments['caseid'].isin(datas['caseid'].drop_duplicates())]

    def date_range_np(x, y):
        return pd.date_range(x[0:10], y[0:10])

    func_date_range_np = np.vectorize(date_range_np, otypes=[object])
    datas['住院日期'] = func_date_range_np(datas['入院时间'], datas['出院时间'])
    datas = datas.explode('住院日期').reset_index(drop=True)


    antis = antis.drop_duplicates()
    antis = antis.merge(antis_dict, on='抗菌药物')
    antis = antis.merge(antis_权重, on='抗菌药物通用名', how='left')

    df_res_抗菌药物医嘱 = pd.DataFrame()
    antis_医院 = antis.copy(deep=True)
    antis_医院['医院名称'] = param.hospname
    df_res_抗菌药物医嘱 = antis_医院

    antis['给药方式'] = np.where(antis['给药方式'].isnull(), '', antis['给药方式'])

    antis_手术用药 = antis[antis.apply(lambda x: '术' in x.给药方式, axis=1)]
    antis_非手术用药 = antis[antis.apply(lambda x: '术' not in x.给药方式, axis=1)]

    if antis_手术用药.shape[0] > 0 and opers.shape[0] > 0:
        antis_手术用药 = antis_手术用药.merge(opers[['caseid', '手术开始时间']], on='caseid', how='left')
        antis_手术用药 = antis_手术用药[antis_手术用药.手术开始时间 > antis_手术用药.医嘱开始时间]
        antis_手术用药 = antis_手术用药.sort_values(['caseid', '医嘱开始时间', '手术开始时间'])

        antis_手术用药 = antis_手术用药.groupby(['caseid']).first().reset_index()
        antis_手术用药['医嘱开始时间'] = antis_手术用药['手术开始时间']
        antis_手术用药.drop(columns=['手术开始时间'], inplace=True)
        antis_手术用药['医嘱结束时间'] = np.where(antis_手术用药['医嘱开始时间'] > antis_手术用药['医嘱结束时间'], antis_手术用药['医嘱开始时间'], antis_手术用药['医嘱结束时间'])
        antis = antis_非手术用药.append(antis_手术用药).reset_index(drop=True)

    # 关联术前高等级抗菌药物
    antis = antis.merge(oper_高等级抗菌药物1, left_on='抗菌药物通用名', right_on='高等级抗菌药物1', how='left')
    # 关联术后48小时抗菌药物
    antis = antis.merge(oper_高等级抗菌药物2, left_on='抗菌药物通用名', right_on='高等级抗菌药物2', how='left')
    antis['医嘱开始时间'] = pd.to_datetime(antis['医嘱开始时间'])
    antis['医嘱结束时间'] = pd.to_datetime(antis['医嘱结束时间'])

    # 每日新开医嘱

    antis_每日新开医嘱 = antis[['caseid', '医嘱开始时间']]
    antis_每日新开医嘱['住院日期'] = antis_每日新开医嘱['医嘱开始时间'].map(lambda x: x if pd.isnull(x) else x.date())
    antis_每日新开医嘱['住院日期'] = pd.to_datetime(antis_每日新开医嘱['住院日期'])
    antis_每日新开医嘱

    # 医嘱开始结束时间拆分开每一天
    antis['住院日期'] = func_date_range_np(antis['医嘱开始时间'].astype(str), antis['医嘱结束时间'].astype(str))
    antis = antis.explode('住院日期').reset_index(drop=True)
    antis['抗菌药物通用名'] = ',' + antis['抗菌药物通用名']

    # 抗菌药物使用总天数
    antis_用药天数 = antis[['caseid', '住院日期']].drop_duplicates().groupby(['caseid']).count().reset_index()
    antis_用药天数.rename(columns={'住院日期': '抗菌药物使用总天数'}, inplace=True)
    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(antis_用药天数, on='caseid', how='left')
    df_感染性事件数据质量表['抗菌药物使用总天数'] = df_感染性事件数据质量表['抗菌药物使用总天数'].fillna(0)

    # 每日抗菌药物、数量、权重和
    antis['抗菌药物通用名'] = antis['抗菌药物通用名'].fillna(',')
    antis_每日抗菌药物 = antis[['caseid', '住院日期', '抗菌药物通用名', '权重']].drop_duplicates().sort_values(['caseid', '住院日期', '抗菌药物通用名']).groupby(['caseid', '住院日期'])['抗菌药物通用名', '权重'].agg(['count', 'sum']).reset_index()
    antis_每日抗菌药物.drop(columns=[('权重', 'count')], inplace=True)
    antis_每日抗菌药物.columns = ['caseid', '住院日期', '抗菌药物种类数', '抗菌药物通用名', '抗菌药物权重和']
    antis_每日抗菌药物.sort_values(['caseid', '住院日期', '抗菌药物通用名'])
    datas = datas.merge(antis_每日抗菌药物, left_on=['caseid', '住院日期'], right_on=['caseid', '住院日期'], how='left')

    # 前后一天抗菌药物、种类数、权重和
    datas_前一天 = datas[['caseid', '住院日期', '抗菌药物种类数', '抗菌药物通用名', '抗菌药物权重和']]
    datas_前一天.columns = ['caseid', '住院日期', '前一天抗菌药物种类数', '前一天抗菌药物通用名', '前一天抗菌药物权重和']
    datas_前一天['住院日期'] = datas_前一天['住院日期'] + timedelta(days=1)

    datas_后一天 = datas[['caseid', '住院日期', '抗菌药物种类数', '抗菌药物通用名', '抗菌药物权重和']]
    datas_后一天.columns = ['caseid', '住院日期', '后一天抗菌药物种类数', '后一天抗菌药物通用名', '后一天抗菌药物权重和']
    datas_后一天['住院日期'] = datas_后一天['住院日期'] - timedelta(days=1)

    datas = datas.merge(datas_前一天, left_on=['caseid', '住院日期'], right_on=['caseid', '住院日期'], how='left')
    datas = datas.merge(datas_后一天, left_on=['caseid', '住院日期'], right_on=['caseid', '住院日期'], how='left')

    datas['是否换药'] = '否'
    datas['是否升级'] = '否'
    datas['抗菌药物种类数'] = datas['抗菌药物种类数'].replace(np.nan, 0).astype('i8')
    datas['抗菌药物通用名'] = datas['抗菌药物通用名'].replace(np.nan, '')
    datas['抗菌药物权重和'] = datas['抗菌药物权重和'].replace(np.nan, 0).astype('i8')

    datas['前一天抗菌药物种类数'] = datas['前一天抗菌药物种类数'].replace(np.nan, 0).astype('i8')
    datas['前一天抗菌药物通用名'] = datas['前一天抗菌药物通用名'].replace(np.nan, '')
    datas['前一天抗菌药物权重和'] = datas['前一天抗菌药物权重和'].replace(np.nan, 0).astype('i8')

    datas['后一天抗菌药物种类数'] = datas['后一天抗菌药物种类数'].replace(np.nan, 0).astype('i8')
    datas['后一天抗菌药物通用名'] = datas['后一天抗菌药物通用名'].replace(np.nan, '')
    datas['后一天抗菌药物权重和'] = datas['后一天抗菌药物权重和'].replace(np.nan, 0).astype('i8')

    datas['抗菌药物前今后'] = datas.apply(lambda x: '是' if set(x.抗菌药物通用名) == set(x.前一天抗菌药物通用名 + x.后一天抗菌药物通用名) else '否', axis=1)
    datas_出入院日期 = datas[['caseid', '住院日期']].groupby(['caseid']).agg(['min', 'max']).reset_index()
    datas_出入院日期.columns = ['caseid', '入院日期', '出院日期']
    datas = datas.merge(datas_出入院日期, on='caseid', how='left')

    ndatas = datas.values
    cond_是否为第一天 = ndatas[:, 6] == ndatas[:, -2]
    cond_是否为非第一天和最后一天 = (ndatas[:, 6] != ndatas[:, -2]) & (ndatas[:, 6] != ndatas[:, -1])
    cond_当天用药不同前一天 = ndatas[:, 8] != ndatas[:, 11]  # 抗菌药物通用名!=前一天抗菌药物通用名
    cond_当一天未用药 = ndatas[:, 7] == 0  # 抗菌药物种类数==0
    cond_前一天未用药 = ndatas[:, 10] == 0  # 前一天抗菌药物种类数==0
    cond_后一天未用药 = ndatas[:, 13] == 0  # 后一天抗菌药物种类数==0
    cond_当天权重大于前一天 = (ndatas[:, 7] > ndatas[:, 10]) | (
                ndatas[:, 9] > ndatas[:, 12])  # 抗菌药物种类数>前一天抗菌药物种类数 | 抗菌药物权重和>前一天抗菌药物权重和
    cond_当天药物等于前后两天 = (ndatas[:, -3] == '是') & (ndatas[:, 7] > ndatas[:, 10]) & (ndatas[:, 7] > ndatas[:,
                                                                                                13])  # 抗菌药物通用名==前一天抗菌药物通用名+后一天抗菌药物通用名 去重  & 抗菌药物种类数>前一天抗菌药物种类数 & 抗菌药物种类数>后一天抗菌药物种类数
    cond_后一天权重大于前一天 = (ndatas[:, 13] > ndatas[:, 10]) | (
                ndatas[:, 15] > ndatas[:, 12])  # 后一天抗菌药物种类数>前一天抗菌药物种类数 | 后一天抗菌药物权重和>前一天抗菌药物权重和

    # 当日是否换药和抗菌药物升降级处理
    ndatas[:, [-5, -4]] = np.where(cond_是否为第一天[:, None],
                                   ['否', '否'],
                                   np.where(cond_是否为非第一天和最后一天[:, None],
                                            np.where(cond_当天用药不同前一天[:, None],
                                                     np.where(cond_前一天未用药[:, None],
                                                              ['否', '是'],
                                                              np.where(cond_当一天未用药[:, None],
                                                                       ['否', '否'],
                                                                       np.where(cond_后一天未用药[:, None],
                                                                                np.where(cond_当天权重大于前一天[:, None],
                                                                                         ['否', '是'],
                                                                                         ['否', '否']
                                                                                         ),
                                                                                np.where(cond_当天药物等于前后两天[:, None],
                                                                                         np.where(
                                                                                             cond_后一天权重大于前一天[:, None],
                                                                                             ['是', '是'],
                                                                                             ['是', '否']
                                                                                         ),
                                                                                         np.where(
                                                                                             cond_当天权重大于前一天[:, None],
                                                                                             ['否', '是'],
                                                                                             ['否', '否']
                                                                                         )
                                                                                         )
                                                                                )
                                                                       )
                                                              ),
                                                     ['否', '否']
                                                     ),
                                            np.where(cond_当天权重大于前一天[:, None],
                                                     ['否', '是'],
                                                     ['否', '否']
                                                     )
                                            )
                                   )

    datas = pd.DataFrame(ndatas, columns=datas.columns)

    # 抗菌药物升级次数
    antis_升级次数 = datas[['caseid', '是否升级']]
    antis_升级次数 = antis_升级次数[antis_升级次数['是否升级'] == '是']
    antis_升级次数 = antis_升级次数.groupby(['caseid']).count().reset_index()
    antis_升级次数.rename(columns={'是否升级': '抗菌药物升级次数'}, inplace=True)

    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(antis_升级次数, on='caseid', how='left')
    df_感染性事件数据质量表['抗菌药物升级次数'] = df_感染性事件数据质量表['抗菌药物升级次数'].fillna(0)

    # 每日最高体温
    temps['住院日期'] = temps['测量时间'].map(lambda x: x if pd.isnull(x) else x[0:10])
    temps_每日最高体温 = temps[['caseid', '住院日期', '体温']].groupby(['caseid', '住院日期'])[['体温']].max()
    temps_每日最高体温 = temps_每日最高体温.reset_index()
    temps_每日最高体温['住院日期'] = pd.to_datetime(temps_每日最高体温['住院日期'])
    datas = datas.merge(temps_每日最高体温, left_on=['caseid', '住院日期'], right_on=['caseid', '住院日期'], how='left')

    # 体温异常次数
    temps_异常次数 = temps[['caseid', '体温']]
    temps_异常次数 = temps_异常次数[temps_异常次数['体温'] >= 38]
    temps_异常次数 = temps_异常次数.groupby(['caseid']).count().reset_index()
    temps_异常次数.rename(columns={'体温': '体温异常次数'}, inplace=True)

    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(temps_异常次数, on='caseid', how='left')
    df_感染性事件数据质量表['体温异常次数'] = df_感染性事件数据质量表['体温异常次数'].fillna(0)

    # 住院时长
    datas['入院时间'] = pd.to_datetime(datas['入院时间'])
    datas['出院时间'] = pd.to_datetime(datas['出院时间'])
    datas_住院天数 = datas[['caseid', '入院时间', '出院时间']].drop_duplicates()
    datas_住院天数['住院时长'] = pd.to_datetime(datas['出院时间']) - pd.to_datetime(datas['入院时间'])
    datas_住院天数['住院时长'] = datas_住院天数['住院时长'].map(lambda x: round(x.days, 2) if x.seconds == 0 else round(x.days + (x.seconds / 20 / 60 / 60), 2))

    datas = datas.merge(datas_住院天数[['caseid', '住院时长']], on='caseid')

    # 手术开始时间 一个人多台手术合并为一行
    df_opers = opers[['caseid', '手术开始时间', '手术名称', '手术结束时间']]
    df_opers = df_opers.fillna('空')
    opers['手术名称'] = opers['手术名称'].fillna('空')

    def concat_func(x):
        return pd.Series({
            '手术开始时间': ','.join(x['手术开始时间']),
            '手术结束时间': ','.join(x['手术结束时间'])
        }
        )

    # 分组聚合+拼接
    df_opers = df_opers.groupby(df_opers['caseid']).apply(concat_func).reset_index()
    # 结果展示
    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(df_opers, on='caseid', how='left')

    opers_手术次数 = pd.DataFrame(opers.groupby(['caseid'])['手术开始时间'].count())
    opers_多台手术患者 = opers_手术次数[opers_手术次数['手术开始时间'] > 1].reset_index()
    opers_多台手术患者_手术信息 = opers[opers['caseid'].isin(opers_多台手术患者['caseid'])].reset_index(drop=True)
    opers_单台手术患者_手术信息 = opers[~opers['caseid'].isin(opers_多台手术患者['caseid'])].reset_index(drop=True)
    opers_多台手术患者_手术信息.手术开始时间 = pd.to_datetime(opers_多台手术患者_手术信息['手术开始时间'])

    opers_多台手术患者_手术信息.手术结束时间 = pd.to_datetime(opers_多台手术患者_手术信息['手术结束时间'])
    opers_多台手术患者_手术信息['下一次手术时间'] = now_time
    opers_多台手术患者_手术信息

    if opers_多台手术患者_手术信息.shape[0] > 0:
        # 多台手术患者，手术结转
        opers_结转_lis1 = []
        opers_结转_lis2 = []
        opers_结转_lis3 = []
        opers_结转_lis4 = []
        opers_结转_lis5 = []

        ind = opers_多台手术患者_手术信息.loc[0]

        for i1, r in opers_多台手术患者_手术信息.iterrows():
            if i1 == 0:
                continue
            if r.caseid == ind.caseid:
                if ind.手术结束时间 + timedelta(days=2) >= r.手术开始时间:
                    ind.手术结束时间 = r.手术开始时间
                    ind.手术名称 = ind.手术名称 + ',' + r.手术名称
                else:
                    ind.下一次手术时间 = r.手术开始时间
                    opers_结转_lis1.extend([ind['caseid']])
                    opers_结转_lis2.extend([ind['手术开始时间'].strftime('%Y-%m-%d %H:%M:%S')])
                    opers_结转_lis3.extend([ind['手术名称']])
                    opers_结转_lis4.extend([ind['手术结束时间'].strftime('%Y-%m-%d %H:%M:%S')])
                    opers_结转_lis5.extend([ind['下一次手术时间'].strftime('%Y-%m-%d %H:%M:%S')])
                    ind = r
            else:
                opers_结转_lis1.extend([ind['caseid']])
                opers_结转_lis2.extend([ind['手术开始时间'].strftime('%Y-%m-%d %H:%M:%S')])
                opers_结转_lis3.extend([ind['手术名称']])
                opers_结转_lis4.extend([ind['手术结束时间'].strftime('%Y-%m-%d %H:%M:%S')])
                opers_结转_lis5.extend([ind['下一次手术时间'].strftime('%Y-%m-%d %H:%M:%S')])
                ind = r

        opers_结转 = pd.DataFrame(
            {'caseid': opers_结转_lis1, '手术开始时间': opers_结转_lis2, '手术名称': opers_结转_lis3, '手术结束时间': opers_结转_lis4,
             '下一次手术时间': opers_结转_lis5})
        opers_结转 = opers_结转.drop_duplicates().reset_index(drop=True)
    else:
        opers_结转 = pd.DataFrame(columns=['caseid', '手术开始时间', '手术名称', '手术结束时间', '下一次手术时间'])
    opers_单台手术患者_手术信息['下一次手术'] = now_time

    # 合并单台手术信息和多台手术结转后的手术信息
    if opers_结转.shape[0] > 0:
        opers_合并 = opers_结转.append(opers_单台手术患者_手术信息)
        opers_合并 = opers_合并.reset_index(drop=True)
    else:
        opers_合并 = opers_单台手术患者_手术信息

    opers_合并['手术开始时间'] = pd.to_datetime(opers_合并['手术开始时间'])
    opers_合并['手术结束时间'] = pd.to_datetime(opers_合并['手术结束时间'])
    opers_合并['下一次手术'] = pd.to_datetime(opers_合并['下一次手术'])

    opers_合并['手术前一天'] = opers_合并['手术开始时间'] - timedelta(days=1)
    opers_合并['手术后两天'] = opers_合并['手术结束时间'] + timedelta(days=2)
    opers_合并['下台手术前一天'] = opers_合并['下一次手术'] - timedelta(days=1)

    antis_高等级用药医嘱 = antis[['caseid', '抗菌药物通用名', '医嘱开始时间', '医嘱结束时间', '是否高等级用药1', '是否高等级用药2']].drop_duplicates()
    antis_高等级用药医嘱 = antis_高等级用药医嘱[(antis_高等级用药医嘱.是否高等级用药1 == 1) | (antis_高等级用药医嘱.是否高等级用药2 == 1)].reset_index(drop=True)

    opers_高等级用药合并 = opers_合并.merge(antis_高等级用药医嘱, on='caseid', how='left')
    opers_高等级用药合并['术前一天使用高等级药物'] = np.where(
        (opers_高等级用药合并['是否高等级用药1'] == 1) & (opers_高等级用药合并['手术前一天'] <= opers_高等级用药合并['医嘱结束时间']) & (
                    opers_高等级用药合并['手术开始时间'] > opers_高等级用药合并['医嘱开始时间']), 1, 0)
    opers_高等级用药合并['术后两天使用高等级药物'] = np.where(
        (opers_高等级用药合并['是否高等级用药2'] == 1) & (opers_高等级用药合并['手术后两天'] < opers_高等级用药合并['医嘱结束时间']) & (
                opers_高等级用药合并['下台手术前一天'] >= opers_高等级用药合并['医嘱开始时间']), 1, 0)

    opers_高等级用药合并 = opers_高等级用药合并[['caseid', '手术开始时间', '术前一天使用高等级药物', '术后两天使用高等级药物']].groupby(['caseid', '手术开始时间'])[
        ['术前一天使用高等级药物', '术后两天使用高等级药物']].sum().reset_index()

    opers_高等级用药合并 = opers_合并.merge(opers_高等级用药合并, left_on=['caseid', '手术开始时间'], right_on=['caseid', '手术开始时间'], how='left')

    temps = temps[temps['体温'] >= 38]
    temps['测量时间'] = pd.to_datetime(temps['测量时间'])
    temps['住院日期'] = pd.to_datetime(temps['住院日期'])

    temps1 = temps.groupby(['caseid', '住院日期'])[['测量时间']].max().merge(
        temps.groupby(['caseid', '住院日期'])[['测量时间']].min().reset_index(), on=['caseid', '住院日期'])
    temps1.rename(columns={'测量时间_x': '当日最小体温大于38度时间', '测量时间_y': '当日最大体温大于38度时间'}, inplace=True)

    opers_高等级用药体温合并 = opers_高等级用药合并.merge(temps1, left_on=['caseid'], right_on=['caseid'], how='left')
    opers_高等级用药体温合并['术后两天体温是否大于38'] = np.where(
        (opers_高等级用药体温合并['住院日期'] is not np.nan) & (opers_高等级用药体温合并['手术后两天'] < opers_高等级用药体温合并['当日最大体温大于38度时间']) & (
                opers_高等级用药体温合并['下台手术前一天'] >= opers_高等级用药体温合并['当日最小体温大于38度时间']), 1, 0)

    opers_高等级用药体温合并 = opers_高等级用药体温合并[['caseid', '手术开始时间', '术后两天体温是否大于38']].groupby(['caseid', '手术开始时间'])[
        ['术后两天体温是否大于38']].sum().reset_index()

    opers_高等级用药体温合并 = opers_高等级用药合并.merge(opers_高等级用药体温合并, left_on=['caseid', '手术开始时间'], right_on=['caseid', '手术开始时间'],
                                          how='left')

    antis_每日新开医嘱1 = antis_每日新开医嘱.groupby(['caseid', '住院日期'])[['医嘱开始时间']].max().merge(
        antis_每日新开医嘱.groupby(['caseid', '住院日期'])[['医嘱开始时间']].min().reset_index(), on=['caseid', '住院日期'])
    antis_每日新开医嘱1.rename(columns={'医嘱开始时间_x': '当日最小新开医嘱时间', '医嘱开始时间_y': '当日最大新开医嘱时间'}, inplace=True)

    opers_高等级用药体温合并['手术后两天日期'] = opers_高等级用药体温合并['手术后两天'].map(lambda x: x.date())
    opers_高等级用药体温合并['手术后两天日期'] = pd.to_datetime(opers_高等级用药体温合并['手术后两天日期'])
    opers_高等级用药体温合并['下台手术前一天日期'] = opers_高等级用药体温合并['下台手术前一天'].map(lambda x: x.date())
    opers_高等级用药体温合并['下台手术前一天日期'] = pd.to_datetime(opers_高等级用药体温合并['下台手术前一天日期'])

    opers_高等级用药体温新开医嘱合并 = opers_高等级用药体温合并.merge(antis_每日新开医嘱1, left_on=['caseid', '手术后两天日期'],
                                                right_on=['caseid', '住院日期'], how='left')
    opers_高等级用药体温新开医嘱合并.rename(columns={'当日最小新开医嘱时间': '术后48h最小新开医嘱时间', '当日最大新开医嘱时间': '术后48h最大新开医嘱时间'}, inplace=True)

    opers_高等级用药体温新开医嘱合并 = opers_高等级用药体温新开医嘱合并.merge(antis_每日新开医嘱1, left_on=['caseid', '下台手术前一天日期'],
                                                    right_on=['caseid', '住院日期'], how='left')
    opers_高等级用药体温新开医嘱合并.rename(columns={'当日最小新开医嘱时间': '下台手术前一天最小新开医嘱时间', '当日最大新开医嘱时间': '下台手术前一天最大新开医嘱时间'}, inplace=True)

    opers_高等级用药体温新开医嘱合并.drop(columns=['住院日期_y', '住院日期_x'], inplace=True)

    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并 = opers_高等级用药体温新开医嘱合并.merge(datas[['caseid', '住院日期', '是否升级']], on=['caseid'],
                                                             how='left')
    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['下台手术前一天最小新开医嘱时间'] = opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['下台手术前一天最小新开医嘱时间'].replace(np.nan,
                                                                                                              now_time)
    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['术后48h最大新开医嘱时间'] = opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['术后48h最大新开医嘱时间'].replace(np.nan,
                                                                                                          min_time)
    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['术后48h最大新开医嘱时间'] = pd.to_datetime(opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['术后48h最大新开医嘱时间'])
    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['下台手术前一天最小新开医嘱时间'] = pd.to_datetime(opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['下台手术前一天最小新开医嘱时间'])

    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['术后第三天至下次手术前第二天是否抗菌药物升级'] = np.where(
        (opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['手术后两天'] < opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['住院日期']) & (
                    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['下台手术前一天'] > opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['住院日期'])
        & (opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['是否升级'] == '是'), 1, 0)

    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['术后第二天是否抗菌药物升级'] = np.where(
        (opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['手术后两天日期'] == opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['住院日期']) & (
                    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['术后48h最大新开医嘱时间'] > opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['手术后两天']) & (
                opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['是否升级'] == '是'), 1, 0)

    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['下次手术前一天是否抗菌药物升级'] = np.where(
        (opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['下台手术前一天日期'] == opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['住院日期']) & (
                    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['下台手术前一天最小新开医嘱时间'] <= opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['下台手术前一天']) & (
                opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['是否升级'] == '是'), 1, 0)

    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['术后48小时是否存在抗菌药物升级'] = np.where(
        (opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['术后第三天至下次手术前第二天是否抗菌药物升级'] == 1) | (
                    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['术后第二天是否抗菌药物升级'] == 1) | (
                    opers_高等级用药体温新开医嘱_抗菌药物是否升级合并['下次手术前一天是否抗菌药物升级'] == 1),
        1, 0)

    opers_高等级用药体温抗菌药物升级合并 = opers_高等级用药体温合并.merge(
        opers_高等级用药体温新开医嘱_抗菌药物是否升级合并[['caseid', '手术开始时间', '术后48小时是否存在抗菌药物升级']].groupby(
            ['caseid', '手术开始时间'])[['术后48小时是否存在抗菌药物升级']].sum().reset_index(), left_on=['caseid', '手术开始时间'],
        right_on=['caseid', '手术开始时间'], how='left')

    opers_高等级用药体温抗菌药物升级合并.drop(columns=['手术后两天日期', '下台手术前一天日期'], inplace=True)

    # 若术前1天使用高等级药物，则将术后48小时升级置为0
    opers_高等级用药体温抗菌药物升级合并['术后48小时是否存在抗菌药物升级'] = np.where(opers_高等级用药体温抗菌药物升级合并['术前一天使用高等级药物'] == 1, 0,
                                                         opers_高等级用药体温抗菌药物升级合并['术后48小时是否存在抗菌药物升级'])

    # 术后两天发热且术后两天抗菌药物升级  或 术后两天为不发热且术后两天使用了高等级药物
    opers_高等级用药体温抗菌药物升级合并['是否院内感染'] = np.where(
        ((opers_高等级用药体温抗菌药物升级合并['术后两天体温是否大于38'] > 0) & (opers_高等级用药体温抗菌药物升级合并['术后48小时是否存在抗菌药物升级'] > 0)) | (
                    (opers_高等级用药体温抗菌药物升级合并['术后两天体温是否大于38'] == 0) & (opers_高等级用药体温抗菌药物升级合并['术后两天使用高等级药物'] > 0)), 1, 0)

    # 手术患者住院过程中是否院内感染
    df_手术患者是否院内感染 = opers_高等级用药体温抗菌药物升级合并.groupby(['caseid'])[['是否院内感染']].sum().reset_index()

    df_手术患者是否院内感染['是否院内感染'] = np.where(df_手术患者是否院内感染['是否院内感染'] > 0, 1, 0)

    # 手术患者每一台手术的是否院内感染字段替换为该患者住院过程是否感染
    opers_高等级用药体温抗菌药物升级合并 = opers_高等级用药体温抗菌药物升级合并.merge(df_手术患者是否院内感染, on='caseid', how='left')
    opers_高等级用药体温抗菌药物升级合并.drop(columns=['是否院内感染_x'], inplace=True)
    opers_高等级用药体温抗菌药物升级合并.rename(columns={'是否院内感染_y': '是否院内感染'}, inplace=True)

    opers_高等级用药体温抗菌药物升级合并['住院日期'] = opers_高等级用药体温抗菌药物升级合并['手术开始时间'].map(
        lambda x: x if pd.isnull(x) else pd.to_datetime(str(x.date())))

    datas_非手术患者明细 = datas.merge(df_感染性事件数据质量表[df_感染性事件数据质量表['是否存在手术信息'] != '是']['caseid'].drop_duplicates(),
                                on='caseid')

    # 入院三天体温大于38天数统计
    datas_非手术患者明细['入院第三天'] = datas_非手术患者明细['入院日期'] + timedelta(days=2)
    temp_入院三天内是否存在体温38 = datas_非手术患者明细[(datas_非手术患者明细['住院日期'] <= datas_非手术患者明细['入院第三天']) & (datas_非手术患者明细['体温'] >= 38)][
        ['caseid']].drop_duplicates().groupby(['caseid'])[['caseid']].count()
    temp_入院三天内是否存在体温38.columns = ['入院三天内是否体温38']
    temp_入院三天内是否存在体温38 = temp_入院三天内是否存在体温38.reset_index()
    datas_非手术患者明细 = datas_非手术患者明细.merge(temp_入院三天内是否存在体温38, on='caseid', how='left')
    datas_非手术患者明细['入院三天内是否体温38'] = datas_非手术患者明细['入院三天内是否体温38'].replace(np.nan, 0).astype('i8')

    # 入院前三天是否使用抗菌药物统计
    temp_入院三天内是否使用抗菌药物 = \
    datas_非手术患者明细[(datas_非手术患者明细['住院日期'] <= datas_非手术患者明细['入院第三天']) & (datas_非手术患者明细['抗菌药物种类数'] > 0)][
        ['caseid']].drop_duplicates().groupby(['caseid'])[['caseid']].count()
    temp_入院三天内是否使用抗菌药物.columns = ['入院三天内是否使用抗菌药物']
    temp_入院三天内是否使用抗菌药物 = temp_入院三天内是否使用抗菌药物.reset_index()
    datas_非手术患者明细 = datas_非手术患者明细.merge(temp_入院三天内是否使用抗菌药物, on='caseid', how='left')
    datas_非手术患者明细['入院三天内是否使用抗菌药物'] = datas_非手术患者明细['入院三天内是否使用抗菌药物'].replace(np.nan, 0).astype('i8')

    # 入院十四天后是否体温大于38统计
    datas_非手术患者明细['入院第十四天'] = datas_非手术患者明细['入院日期'] + timedelta(days=6)
    temp_入院十四天后是否存在体温38 = \
    datas_非手术患者明细[(datas_非手术患者明细['住院日期'] > datas_非手术患者明细['入院第十四天']) & (datas_非手术患者明细['体温'] >= 38)][
        ['caseid']].drop_duplicates().groupby(['caseid'])[['caseid']].count()
    temp_入院十四天后是否存在体温38.columns = ['入院十四天后是否体温38']
    temp_入院十四天后是否存在体温38 = temp_入院十四天后是否存在体温38.reset_index()

    datas_非手术患者明细 = datas_非手术患者明细.merge(temp_入院十四天后是否存在体温38, on='caseid', how='left')
    datas_非手术患者明细['入院十四天后是否体温38'] = datas_非手术患者明细['入院十四天后是否体温38'].replace(np.nan, 0).astype('i8')

    # 入院十四天后是否抗菌药物升级统计
    temp_入院十四天后是否存在抗菌药物升级 = \
    datas_非手术患者明细[(datas_非手术患者明细['住院日期'] > datas_非手术患者明细['入院第十四天']) & (datas_非手术患者明细['是否升级'] == '是')][
        ['caseid']].drop_duplicates().groupby(['caseid'])[['caseid']].count()
    temp_入院十四天后是否存在抗菌药物升级.columns = ['入院十四天后是否抗菌药物升级']
    temp_入院十四天后是否存在抗菌药物升级 = temp_入院十四天后是否存在抗菌药物升级.reset_index()

    datas_非手术患者明细 = datas_非手术患者明细.merge(temp_入院十四天后是否存在抗菌药物升级, on='caseid', how='left')
    datas_非手术患者明细['入院十四天后是否抗菌药物升级'] = datas_非手术患者明细['入院十四天后是否抗菌药物升级'].replace(np.nan, 0).astype('i8')

    # 入院三天后是否存在体温38统计

    temp_入院三天后是否存在体温38 = datas_非手术患者明细[(datas_非手术患者明细['住院日期'] > datas_非手术患者明细['入院第三天']) & (datas_非手术患者明细['体温'] >= 38)][
        ['caseid']].drop_duplicates().groupby(['caseid'])[['caseid']].count()
    temp_入院三天后是否存在体温38.columns = ['入院三天后是否存在体温38']
    temp_入院三天后是否存在体温38 = temp_入院三天后是否存在体温38.reset_index()

    datas_非手术患者明细 = datas_非手术患者明细.merge(temp_入院三天后是否存在体温38, on='caseid', how='left')
    datas_非手术患者明细['入院三天后是否存在体温38'] = datas_非手术患者明细['入院三天后是否存在体温38'].replace(np.nan, 0).astype('i8')

    # 入院三天后是否抗菌药物升级统计

    temp_入院三天后是否存在抗菌药物升级 = \
    datas_非手术患者明细[(datas_非手术患者明细['住院日期'] > datas_非手术患者明细['入院第三天']) & (datas_非手术患者明细['是否升级'] == '是')][
        ['caseid']].drop_duplicates().groupby(['caseid'])[['caseid']].count()
    temp_入院三天后是否存在抗菌药物升级.columns = ['入院三天后是否抗菌药物升级']
    temp_入院三天后是否存在抗菌药物升级 = temp_入院三天后是否存在抗菌药物升级.reset_index()

    datas_非手术患者明细 = datas_非手术患者明细.merge(temp_入院三天后是否存在抗菌药物升级, on='caseid', how='left')
    datas_非手术患者明细['入院三天后是否抗菌药物升级'] = datas_非手术患者明细['入院三天后是否抗菌药物升级'].replace(np.nan, 0).astype('i8')

    datas_非手术患者明细['是否院内感染'] = np.where(((datas_非手术患者明细['入院三天内是否体温38'] == 1) & (datas_非手术患者明细['入院三天内是否使用抗菌药物'] == 1)),
                                       np.where(((datas_非手术患者明细['入院十四天后是否体温38'] == 1) & (
                                                   datas_非手术患者明细['入院十四天后是否抗菌药物升级'] == 1)), 1, 0),
                                       np.where(((datas_非手术患者明细['入院三天后是否存在体温38'] == 1) & (
                                                   datas_非手术患者明细['入院三天后是否抗菌药物升级'] == 1)), 1, 0)
                                       )

    datas_非手术患者 = datas_非手术患者明细.groupby(['caseid'])[['是否院内感染']].sum().reset_index()
    datas_非手术患者['是否院内感染'] = np.where(datas_非手术患者['是否院内感染'] > 0, 1, 0)
    datas_非手术患者明细 = datas_非手术患者明细.merge(datas_非手术患者, on='caseid', how='left')
    datas_非手术患者明细.drop(columns=['是否院内感染_x'], inplace=True)
    datas_非手术患者明细.rename(columns={'是否院内感染_y': '是否院内感染'}, inplace=True)

    datas_手术患者明细 = datas[datas['caseid'].isin(opers_高等级用药体温抗菌药物升级合并['caseid'])]
    datas_手术患者明细 = datas_手术患者明细.merge(opers_高等级用药体温抗菌药物升级合并, on=['caseid', '住院日期'], how='left').reset_index(drop=True)
    datas_手术患者明细.drop(columns=['入院时间_y', '出院时间_y'], inplace=True)
    datas_手术患者明细.rename(columns={'入院时间_x': '入院时间', '出院时间_x': '出院时间'}, inplace=True)

    df_是否院内感染患者汇总 = df_手术患者是否院内感染.append(datas_非手术患者).drop_duplicates()

    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(df_是否院内感染患者汇总, on='caseid', how='left')

    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(datas_住院天数[['caseid', '住院时长']], on='caseid', how='left')

    def reason(x):
        if pd.isnull(x.是否院内感染):
            res = ''
            if x.出院时间是否存在问题 == '是':
                res = res + '出院时间存在问题，'
            if (~pd.isnull(x.医嘱开始结束时间存在问题次数)) and x.医嘱开始结束时间存在问题次数 >= 1:
                res = res + '医嘱开始结束时间存在问题，'
            if (~pd.isnull(x.手术开始结束时间存在问题次数)) and x.手术开始结束时间存在问题次数 >= 1:
                res = res + '手术开始结束时间存在问题,'
            if (~pd.isnull(x.体温信息测量时间存在问题次数)) and x.体温信息测量时间存在问题次数 >= 1:
                res = res + '体温信息存在问题'
            return res
        else:
            return '该患者进行了感染性事件计算'

    df_感染性事件数据质量表['患者未进行感染性事件计算原因'] = df_感染性事件数据质量表.apply(lambda x: reason(x), axis=1)

    datas_手术患者明细['医院名称'] = df_医院名称
    datas_非手术患者明细['医院名称'] = df_医院名称


    datas_手术患者明细_数据质量需求 = datas_手术患者明细[['caseid', '术后两天使用高等级药物', '术后两天体温是否大于38', '术后48小时是否存在抗菌药物升级']]

    datas_手术患者明细_数据质量需求 = datas_手术患者明细_数据质量需求.groupby(['caseid'])[
        ['术后两天使用高等级药物', '术后两天体温是否大于38', '术后48小时是否存在抗菌药物升级']].sum().reset_index()
    datas_手术患者明细_数据质量需求.rename(columns={'术后两天体温是否大于38': '术后两天体温是否大于38次数', '术后48小时是否存在抗菌药物升级': '术后48小时是否存在抗菌药物升级次数'},
                               inplace=True)

    datas_非手术患者明细_数据质量需求 = datas_非手术患者明细[['caseid', '入院三天后是否存在体温38', '入院三天后是否抗菌药物升级']].drop_duplicates()

    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(datas_手术患者明细_数据质量需求, on=['caseid'], how='left')
    df_感染性事件数据质量表 = df_感染性事件数据质量表.merge(datas_非手术患者明细_数据质量需求, on=['caseid'], how='left')

    time2 = time.time()
    print("结束执行%s-%s,进程号为%d ,进程时长%.2f,结束时间为%s" % (btime, etime, os.getpid(), time2 - time1, pd.datetime.now()))
    return {'感染性事件数据质量表': df_感染性事件数据质量表,
            '手术患者明细': datas_手术患者明细,
            '非手术患者明细': datas_非手术患者明细,
            '抗菌药物医嘱': df_res_抗菌药物医嘱,
            '出院时间存在问题': df_出院时间存在问题,
            '医嘱开始结束时间存在问题': df_医嘱开始结束时间存在问题,
            '手术开始结束时间存在问题': df_手术开始结束时间存在问题,
            '体温信息测量时间存在问题': df_体温信息测量时间存在问题,
            }




def compute_layout():
    return html.Div(
    [
        html.Br(),
        html.Br(),
        dbc.Row(
            [
                dcc.Link(
                    html.Button(
                        "感染性事件计算", id="compute-button", className="mr-4"
                    ),
                    href=f"/dash/compute",
                ),
                dcc.Link(
                    html.Button(
                        "数据结果展示", id="detail-button", className="mr-4"
                    ),
                    href=f"/dash/detail",
                )
            ],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [dbc.Col(width=1),dbc.Col(html.H3("数据库连接"), width=10), dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [   dbc.Col(width=1),
                dbc.Col([
                    dbc.Label(html.B('数据库类型:'),id="db-dropdown-label" ) ,
                     dcc.Dropdown(
                    id="db-dropdown",
                    options=[
                        {"label": "Oracle", "value": "oracle"},
                        {"label": "Mysql", "value": "mysql"},
                    ],
                    value="oracle"
                )],
                width=4
                ),
                dbc.Col(width=2),
                dbc.Col([
                     dbc.Label(html.B('数据库驱动:'),id="dbtype-dropdown-label"  ) ,
                     dcc.Dropdown(
                         id="dbtype-dropdown",
                         options=[
                             {"label": "Oracle驱动", "value": "cx_oracle"},
                             {"label": "Mysql驱动", "value": "mysqldb"},
                         ],
                         value="cx_oracle"
                    )],
                    width=4
                ),
                dbc.Col(width=1),
            ],justify="center",
        ),
        html.Br(),
        html.Br(),
        dbc.Row(
            [   dbc.Col(width=1),
                dbc.Col([
                        dbc.Label(html.B("数据库IP"), html_for="dbhost",id="dbhost-label"),
                        dbc.Input(id="dbhost", placeholder="请输入数据库IP")
                ]),
                dbc.Col([
                        dbc.Label(html.B("数据库端口"), html_for="dbport",id="dbport-label"),
                        dbc.Input(id="dbport", placeholder="请输入数据库端口")
                ]),
                dbc.Col([
                        dbc.Label(html.B("数据库用户名"), html_for="dbuser",id="dbuser-label"),
                        dbc.Input(id="dbuser", placeholder="请输入数据库用户名")
                ]),
                dbc.Col([
                        dbc.Label(html.B("数据库密码"), html_for="dbpassword",id="dbpassword-label"),
                        dbc.Input(id="dbpassword", placeholder="请输入数据库密码",type='password')
                ]),
                dbc.Col([
                        dbc.Label(html.B("数据库实例名"), html_for="dborcl",id="dborcl-label"),
                        dbc.Input(id="dborcl", placeholder="请输入数据库实例名")
                ]),

                dbc.Col(width=1),
            ],justify="center",
        ),
        html.Br(),
        html.Br(),
        dbc.Row(
            [dbc.Col(width=1),dbc.Col(html.H3("数据库连接"), width=10), dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(width=1),
                dbc.Col(
                [
                    dbc.Row([
                        dbc.Label(html.B("患者基本信息"), html_for="cbasics",id="cbasics-label"),
                        dbc.Textarea(value = "select t1.CASEID,t1.IN_TIME as 入院时间,t1.OUT_TIME as 出院时间,t2.LABEL as 入院科室,round((to_date(substr(t1.IN_TIME,1,10), 'YYYY-MM-DD') - to_date(substr(t1.PBIRTHDATE,1,10), 'YYYY-MM-DD')) /365,2) as 年龄 from overall t1,s_departments t2 where t1.IN_TIME between :1 and :2 and t1.in_dept=t2.code",
                                     id="cbasics", placeholder="select t1.CASEID,t1.IN_TIME as 入院时间,t1.OUT_TIME as 出院时间,t2.LABEL as 入院科室,round((to_date(substr(t1.IN_TIME,1,10), 'YYYY-MM-DD') - to_date(substr(t1.PBIRTHDATE,1,10), 'YYYY-MM-DD')) /365,2) as 年龄 from overall t1,s_departments t2 where t1.IN_TIME between :1 and :2 and t1.in_dept=t2.code")
                    ],style={'margin-left':'5px','margin-right':'5px'}),
                ] , width=10),
                dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(width=1),
                dbc.Col(
                [
                    dbc.Row([
                        dbc.Label(html.B("全身给药抗菌药物医嘱信息"), html_for="antis",id="antis-label"),
                        dbc.Textarea(value = "select CASEID,ANAME as 抗菌药物,BEGINTIME as 医嘱开始时间,ENDTIME as 医嘱结束时间,ADMINISTRATION as 给药方式 from ANTIBIOTICS  where caseid in (select CASEID from overall where IN_TIME between :1 and :2 )  order by BEGINTIME",
                                     id="antis", placeholder="select CASEID,ANAME as 抗菌药物,BEGINTIME as 医嘱开始时间,ENDTIME as 医嘱结束时间,ADMINISTRATION as 给药方式 from ANTIBIOTICS  where caseid in (select CASEID from overall where IN_TIME between :1 and :2 )  order by BEGINTIME")
                    ],style={'margin-left':'5px','margin-right':'5px'}),
                ] , width=10),
                dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(width=1),
                dbc.Col(
                [
                    dbc.Row([
                        dbc.Label(html.B("患者手术信息"), html_for="opers",id="opers-label"),
                        dbc.Textarea(value = "select CASEID,OPERID ,BEGINTIME as 手术开始时间,OPER_NAME as 手术名称, ENDTIME as 手术结束时间 from OPER2 where caseid in (select CASEID from overall where IN_TIME between ：1 and ：2) and BEGINTIME<>'0001-01-01 00:00:00' order by BEGINTIME",
                                     id="opers", placeholder="select CASEID,OPERID ,BEGINTIME as 手术开始时间,OPER_NAME as 手术名称, ENDTIME as 手术结束时间 from OPER2 where caseid in (select CASEID from overall where IN_TIME between ：1 and ：2) and BEGINTIME<>'0001-01-01 00:00:00' order by BEGINTIME")
                    ],style={'margin-left':'5px','margin-right':'5px'}),
                ] , width=10),
                dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(width=1),
                dbc.Col(
                [
                    dbc.Row([
                        dbc.Label(html.B("患者体征信息"), html_for="temps",id="temps-label"),
                        dbc.Textarea(value = "select CASEID,VALUE as 体温,RECORDDATE as 测量时间 from TEMPERATURE where caseid in (select CASEID from overall where IN_TIME between :1 and :2) order by RECORDDATE",
                                     id="temps", placeholder="select CASEID,VALUE as 体温,RECORDDATE as 测量时间 from TEMPERATURE where caseid in (select CASEID from overall where IN_TIME between :1 and :2) order by RECORDDATE")
                    ],style={'margin-left':'5px','margin-right':'5px'}),
                ] , width=10),
                dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(width=1),
                dbc.Col(
                [
                    dbc.Row([
                        dbc.Label(html.B("患者菌检出信息"), html_for="bars",id="bars-label"),
                        dbc.Textarea(value = "select CASEID,BACTERIA as 菌检出,REQUESTTIME as 检验申请时间 from BACTERIA where caseid in (select CASEID from overall where IN_TIME between :1 and :2) order by REPORTTIME",
                                     id="bars", placeholder="select CASEID,BACTERIA as 菌检出,REQUESTTIME as 检验申请时间 from BACTERIA where caseid in (select CASEID from overall where IN_TIME between :1 and :2) order by REPORTTIME")
                    ],style={'margin-left':'5px','margin-right':'5px'}),
                ] , width=10),
                dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(width=1),
                dbc.Col(
                [
                    dbc.Row([
                        dbc.Label(html.B("患者转科信息"), html_for="departments",id="departments-label"),
                        dbc.Textarea(value = "select t1.CASEID,t1.入科时间,t2.label as 科室,t1.出科时间 from (select CASEID,BEGINTIME as 入科时间,DEPT as 科室,ENDTIME as 出科时间 from department where caseid in (select CASEID from overall where IN_TIME between :1 and :2) order by BEGINTIME) t1,s_departments t2 where t1.科室=t2.code",
                                     id="departments", placeholder="select t1.CASEID,t1.入科时间,t2.label as 科室,t1.出科时间 from (select CASEID,BEGINTIME as 入科时间,DEPT as 科室,ENDTIME as 出科时间 from department where caseid in (select CASEID from overall where IN_TIME between :1 and :2) order by BEGINTIME) t1,s_departments t2 where t1.科室=t2.code")
                    ],style={'margin-left':'5px','margin-right':'5px'}),
                ] , width=10),
                dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(width=1),
                dbc.Col(
                [
                    dbc.Row([
                        dbc.Label(html.B("抗菌药物名称"), html_for="allantinames",id="allantinames-label"),
                        dbc.Textarea(value = "select distinct ANAME as 抗菌药物 from ANTIBIOTICS where caseid in (select CASEID from  overall where IN_TIME between :1 and :2 )",
                                     id="allantinames", placeholder="select distinct ANAME as 抗菌药物 from ANTIBIOTICS where caseid in (select CASEID from  overall where IN_TIME between :1 and :2 )")
                    ],style={'margin-left':'5px','margin-right':'5px'}),
                ] , width=10),
                dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [dbc.Col(width=1),dbc.Col(html.H3("统计时段信息"), width=10), dbc.Col(width=1),],
            justify="center",
        ),

        html.Br(),
        dbc.Row(
            [   dbc.Col(width=1),
                dbc.Col([
                    dbc.Label(html.B('统计开始时间') ,id="begintime-label") ,
                    dbc.Input(id='begintime',type='date')
                     ],
                width=4
                ),
                dbc.Col(width=2),

                dbc.Col([
                    dbc.Label(html.B('统计结束时间') ,id="endtime-label"),
                    dbc.Input(id='endtime', type='date')
                    ],
                    width=4
                ),
                dbc.Col(width=1),
            ],justify="center",
        ),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(width=1),
                dbc.Col([
                    dbc.Label(html.B("计算进程数") ,id="process-label"),
                    dbc.Input(id="process",placeholder="eg : 1")
                ]),
                dbc.Col(width=1),
            ],justify="center",
        ),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(width=1),
                dbc.Col([
                    dbc.Label(html.B("医院名称"),id="hospname-label"),
                    dbc.Input(id="hospname",placeholder="eg : YCSDYRMYY")
                ]),
                dbc.Col(width=1),
            ],justify="center",
        ),

        html.Br(),
        dbc.Row([dbc.Col(width=1),dbc.Button('计算', id='start-fullscreen'),dbc.Col(width=1)],justify="center"),
        html.Br(),
        dbc.Row([dbc.Col(width=1),dbc.Col(dbc.Spinner(html.Ul(id='error_msg')),width=10),dbc.Col(width=1)],justify="center"),

        html.Br(),
        dbc.Row([dbc.Col(width=1),
                 dcc.RadioItems(
                     id='fig_type',options=[
                        {'label': '总人数', 'value': '总人数'},
                        {'label': '有抗菌药物医嘱患者比例', 'value': '抗菌药物医嘱比例'},
                        {'label': '发热患者比例', 'value': '发热患者比例'},
                        {'label': '手术患者比例', 'value': '手术患者比例'}
                    ], value='总人数'),
                 dbc.Col(width=1)],justify="center"),
        html.Div([dcc.Graph(id='hos_line' )]),
        dbc.Row([dbc.Col(width=1),html.Div(id='intermediate-value',style = {'content-visibility':'hidden'}),dbc.Col(width=1)],justify="center"),
        html.Br(),
        html.Br(),
        html.Br(),
    ]
)

def detail_layout():


    return  html.Div(
    [
        html.Br(),
        html.Br(),
        dbc.Row(
            [
                dcc.Link(
                    html.Button(
                        "感染性事件计算", id="compute-button", className="mr-4"
                    ),
                    href=f"/dash/compute",
                ),
                dcc.Link(
                    html.Button(
                        "数据结果展示", id="detail-button", className="mr-4"
                    ),
                    href=f"/dash/detail",
                )
            ],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [dbc.Col(width=1),dbc.Col(html.H3("展示表选择"), width=10), dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),

        dbc.Row(
            [dbc.Col(width=1),
             dbc.Col(
                dbc.Row(
                    [
                    dbc.Col(
                        dbc.Row(
                                get_upload_component(id='dash-uploader'),
                                id='upload_border',
                    ),width=4),
                    dbc.Col(
                        [
                            dbc.Row(
                                html.A(
                                    dbc.Button('清空', id='clear-upload'),
                                    href=f"/dash/detail",
                                    ),
                                justify="center"),
                            html.Br(),
                            dbc.Row(dbc.Button('合并', id='merge-date'),justify="center")
                        ]
                        ,width=1,style={'margin-top':'0.8%'}),
                    dbc.Col( id = 'merge-date-down',width=6 ,style={'display':'flex','align-items':'center','justify-content':'center'} )
                    ],justify="center",
                ), width=10), dbc.Col(width=1),],
            justify="center",
        ),
        html.Br(),
        dbc.Row(
            [dbc.Col(width=1), dbc.Col(html.H3("数据质量结果展示"), width=10), dbc.Col(width=1), ],
            justify="center",
        ),
        html.Br(),

        dbc.Row(
            [
                dbc.Col(width=1),
                dbc.Col( [
                    dbc.Label(html.B("数据展示类型:"), id="hos-data-type"),
                    dcc.Dropdown(
                        id='event_type',
                        options=[{'label': i, 'value': i} for i in ['未进行计算人数', '未感染性事件计算比例']],
                        value='未进行计算人数'
                    ),
                    # html.Div(dcc.Graph(id='hos_bar')),
                    ] ),
                dbc.Col(width=1),
                dbc.Col([
                    dbc.Label(html.B("医院名称:"), id="hospname-label"),
                    dcc.Dropdown(id='event_hospital',),
                    # html.Div([dcc.Graph(id='hos_pie')])
                ]),
                dbc.Col(width=1),

            ], justify="center",
        ),
        html.Br(),

        dbc.Row(
            [
                dbc.Col(
                    dbc.Row([
                        dbc.Col(width=1),
                        dbc.Col(dcc.Graph(id='hos_bar'), width=11),
                    ], justify="center",) ,width=5),
                dbc.Col(width=1),
                dbc.Col(
                    dbc.Row([
                        dbc.Col(dcc.Graph(id='hos_pie'), width=11),
                        dbc.Col(width=1),
                    ], justify="center", ), width=5),
                # dbc.Col( dcc.Graph(id='hos_pie') ,width=5),
            ], justify="center",
        ),


        html.Br(),
        dbc.Row([
            dbc.Col(width=1),
            dbc.Col([
                dbc.Label(html.B("医院选择:"), id="hos-choice"),
                dcc.Dropdown(id='lis_hos', multi=True),
                html.Br(),
                dcc.RadioItems(
                    id='fig_type',
                    options=[
                        {'label': '总人数', 'value': '总人数'},
                        {'label': '有抗菌药物医嘱患者比例', 'value': '抗菌药物医嘱比例'},
                        {'label': '发热患者比例', 'value': '发热患者比例'},
                        {'label': '手术患者比例', 'value': '手术患者比例'}
                    ],
                    value='总人数'
                ),
            ]),
            dbc.Col(width=1),
        ]),
        html.Br(),
        dbc.Row([
            dbc.Col(width=1),
            dbc.Col(dcc.Graph(id='hos_line_mul'),width=10),
            dbc.Col(width=1),
        ], justify="center",),
        html.Br(),

        dbc.Row([dbc.Col(width=1), html.Div(id='intermediate-value1', style={'content-visibility': 'hidden'}),
                 dbc.Col(width=1)], justify="center"),
        dbc.Row([dbc.Col(width=1), html.Div(id='intermediate-value2', style={'content-visibility': 'hidden'}),
                 dbc.Col(width=1)], justify="center"),
        dbc.Row([dbc.Col(width=1), html.Div(id='intermediate-value3', style={'content-visibility': 'hidden'}),
                 dbc.Col(width=1)], justify="center"),
        html.Br(),
        html.Br(),
        html.Br(),
    ]
)

"""Homepage"""
app.layout = html.Div(
    [ dcc.Location(id="url", refresh=False), html.Div(id="page-content"),]
)

# 页面路由
@app.callback(
    dash.dependencies.Output("page-content", "children"),
    [dash.dependencies.Input("url", "pathname")],
)
def display_page(pathname):
    if pathname.endswith("/compute"):
        return compute_layout()
    elif pathname.endswith("/detail"):
        return detail_layout()


# 文件下载
@app.server.route('/download/<file>')
def download(file):
    # return send_from_directory('NetDisk', file)
    current_path = os.getcwd()
    current_path = current_path + '\\out'
    return send_from_directory(current_path, file)
# 文件下载1
@app.server.route('/download1/<file>')
def download1(file):
    current_path = os.getcwd()
    current_path = current_path + '\\upload'
    return send_from_directory(current_path, file)


# 计算
@app.callback(
    [Output('error_msg','children'),
     Output('error_msg','style'),
     Output('dbhost-label','style'),
     Output('dbhost','style'),
     Output('dbport-label','style'),
     Output('dbport','style'),
     Output('dbuser-label','style'),
     Output('dbuser','style'),
     Output('dbpassword-label','style'),
     Output('dbpassword','style'),
     Output('dborcl-label','style'),
     Output('dborcl','style'),
     Output('cbasics-label','style'),
     Output('cbasics','style'),
     Output('antis-label','style'),
     Output('antis','style'),
     Output('opers-label','style'),
     Output('opers','style'),
     Output('temps-label','style'),
     Output('temps','style'),
     Output('bars-label','style'),
     Output('bars','style'),
     Output('departments-label','style'),
     Output('departments','style'),
     Output('allantinames-label','style'),
     Output('allantinames','style'),
     Output('begintime-label','style'),
     Output('begintime','style'),
     Output('endtime-label','style'),
     Output('endtime','style'),
     Output('process-label','style'),
     Output('process','style'),
     Output('hospname-label','style'),
     Output('hospname','style'),
     Output('intermediate-value','children'),
     ],
    Input('start-fullscreen', 'n_clicks'),
    [State('db-dropdown', 'value'),
     State('dbtype-dropdown', 'value'),
     State('dbhost', 'value'),
     State('dbport', 'value'),
     State('dbuser', 'value'),
     State('dbpassword', 'value'),
     State('dborcl', 'value'),
     State('cbasics', 'value'),
     State('antis', 'value'),
     State('opers', 'value'),
     State('temps', 'value'),
     State('bars', 'value'),
     State('departments', 'value'),
     State('allantinames', 'value'),
     State('begintime', 'value'),
     State('endtime', 'value'),
     State('process', 'value'),
     State('hospname', 'value')
     ],
    prevent_initial_call=True
)
def loading(n_clicks,db,dbtype,dbhost,dbport,dbuser,dbpassword,dborcl,cbasics,antis,opers,temps,bars,departments,allantinames,begintime,endtime,process,hospname):
    print((n_clicks,db,dbtype,dbhost,dbport,dbuser,dbpassword,dborcl,cbasics,antis,opers,temps,bars,departments,allantinames,begintime,endtime,process,hospname))
    lis_style = [[], {}, {}, {}, {}, {},{}, {}, {}, {},{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, ''
                 ]
    vild_lis = [dbhost,dbport,dbuser,dbpassword,dborcl,cbasics,antis,opers,temps,bars,departments,allantinames,begintime,endtime,process,hospname]
    error_msg = ["数据库IP为空","数据库端口为空","数据库用户为空","数据库密码为空","数据库实例名为空","数据库IP为空","患者基本信息为空","全身给药抗菌药物医嘱信息为空","患者手术信息为空","患者体征信息为空",
                 "患者菌检出信息为空","患者转科信息为空","抗菌药物名称为空","统计开始时间为空","统计结束时间为空","计算进程数为空","医院名称为空"]
    for i in range(len(vild_lis)):
        if vild_lis[i] is None or len(vild_lis[i]) == 0:
            if len(lis_style[0]) == 0:
                lis_style[0] = [html.Li(error_msg[i])]
            else:
                lis_style[0].append(html.Li(error_msg[i]))
            lis_style[1] = {'color': '#9F3A38', 'border-color': '#9F3A38'}
            lis_style[1 + 2 * i + 1] = {'color': '#9F3A38', 'border-color': '#9F3A38'}
            lis_style[1 + 2 * i + 2] = {'color': '#9F3A38', 'border-color': '#9F3A38'}
    if len(lis_style[0]) > 0:
        return lis_style
    else:
        pp = {
            "dbname": db,
            "dbdriver": dbtype,
            "dbhost": dbhost,
            "dbport": dbport,
            "dbuser": dbuser,
            "dbpasswd": dbpassword,
            "dborcl": dborcl,
            "cbasics": cbasics.replace('\n', ''),
            "antis": antis.replace('\n', ''),
            "opers": opers.replace('\n', ''),
            "temps": temps.replace('\n', ''),
            "bars": bars.replace('\n', ''),
            "departments": departments.replace('\n', ''),
            "allantinames": allantinames.replace('\n', ''),
            "begintime": begintime,
            "endtime": endtime,
            "process": process,
            "hospname": hospname
        }
        param = Param(**pp)
        print(param)

        btime = param.begintime
        etime = param.endtime

        btimes, etimes = list_months(btime, etime)
        now_time = str(str(datetime.now())).replace(' ', '-').replace('.', '-').replace(':', '-')
        # 结果输出目录（当前时间/医院名称/统计时段）
        res_id = str(uuid.uuid1())
        print(res_id,type(res_id))
        dir_name = './out/' + now_time + '/' + res_id + '-' + param.hospname + '-' + btime + '-' + etime
        os.makedirs(dir_name)
        csv_name_手术患者明细 = dir_name + '/' + now_time + '_' + btime + '-' + etime + '手术患者明细.csv'
        csv_name_非手术患者明细 = dir_name + '/' + now_time + '_' + btime + '-' + etime + '非手术患者明细.csv'
        csv_name_感染性事件 = dir_name + '/' + now_time + '_' + btime + '-' + etime + '感染性事件结果.csv'
        excel_name_问题数据明细 = dir_name + '/' + now_time + '_' + btime + '-' + etime + '问题数据明细.xlsx'
        csv_name_未识别抗菌药物 = dir_name + '/' + now_time + '_' + btime + '-' + etime + '未识别抗菌药物.csv'
        csv_name_医嘱明细 = dir_name + '/' + now_time + '_' + btime + '-' + etime + '医嘱明细.csv'

        try:
            engine = create_engine(
                param.dbname + "+" + param.dbdriver + "://" + param.dbuser + ":" + param.dbpasswd + "@" + param.dbhost + ":" + param.dbport + "/" + param.dborcl,
                echo=False, encoding='UTF-8')
        except :
            lis_style[0].append(html.Li("数据库连接有误"))
            lis_style[1] = {'color': '#9F3A38', 'border-color': '#9F3A38'}

        all_antis = pd.read_sql(param.allantinames, params=(btime, etime), con=engine)

        antis_dict = discriminated_antis(all_antis)
        antis_未被识别 = antis_dict[antis_dict['抗菌药物通用名'].isnull()][['抗菌药物']]

        if antis_未被识别.shape[0] > 0:
            print("存在抗菌药物名称未识别！！！文件地址：" + csv_name_未识别抗菌药物)
            antis_未被识别.to_csv(csv_name_未识别抗菌药物, index=False)
        print('计算进行中')

        try:
            res_计算结果 = Parallel(n_jobs=int(param.process), backend="multiprocessing")(
                delayed(bg_compute)(btimes[i], etimes[i], param, antis_dict)
                for i in range(len(btimes)))
        except Exception as  e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print(exc_type)
            print(exc_value)
            print(exc_traceback)
            lis_style[0].append(html.Ul('计算有误: traceback.format_exc():\n%s' % traceback.format_exc()))
            lis_style[1] = {'color': '#9F3A38', 'border-color': '#9F3A38'}
            return lis_style

        df_data_出院时间存在问题 = pd.DataFrame()
        df_data_医嘱开始结束时间存在问题 = pd.DataFrame()
        df_data_手术开始结束时间存在问题 = pd.DataFrame()
        df_data_体温信息测量时间存在问题 = pd.DataFrame()

        for df_data in res_计算结果:
            print(df_data)
            for name in df_data:
                if name == '感染性事件数据质量表':
                    if os.path.exists(csv_name_感染性事件):
                        df_data[name].to_csv(csv_name_感染性事件, index=False, mode='a', encoding='utf-8', header=False)
                    else:
                        df_data[name].to_csv(csv_name_感染性事件, index=False, mode='a', encoding='utf-8')
                elif name == '手术患者明细':
                    if os.path.exists(csv_name_手术患者明细):
                        df_data[name].to_csv(csv_name_手术患者明细, index=False, header=False, encoding='utf-8', mode='a')
                    else:
                        df_data[name].to_csv(csv_name_手术患者明细, index=False, encoding='utf-8', mode='a')
                elif name == '非手术患者明细':
                    if os.path.exists(csv_name_非手术患者明细):
                        df_data[name].to_csv(csv_name_非手术患者明细, index=False, header=False, encoding='utf-8', mode='a')
                    else:
                        df_data[name].to_csv(csv_name_非手术患者明细, index=False, encoding='utf-8', mode='a')
                elif name == '抗菌药物医嘱':
                    if os.path.exists(csv_name_医嘱明细):
                        df_data[name].to_csv(csv_name_医嘱明细, index=False, header=False, encoding='utf-8', mode='a')
                    else:
                        df_data[name].to_csv(csv_name_医嘱明细, index=False, encoding='utf-8', mode='a')
                elif name == '出院时间存在问题':
                    if df_data_出院时间存在问题.shape[0] == 0:
                        df_data_出院时间存在问题 = df_data[name]
                    else:
                        df_data_出院时间存在问题 = df_data_出院时间存在问题.append(df_data[name])

                elif name == '医嘱开始结束时间存在问题':
                    if df_data_医嘱开始结束时间存在问题.shape[0] == 0:
                        df_data_医嘱开始结束时间存在问题 = df_data[name]
                    else:
                        df_data_医嘱开始结束时间存在问题 = df_data_医嘱开始结束时间存在问题.append(df_data[name])

                elif name == '手术开始结束时间存在问题':
                    if df_data_手术开始结束时间存在问题.shape[0] == 0:
                        df_data_手术开始结束时间存在问题 = df_data[name]
                    else:
                        df_data_手术开始结束时间存在问题 = df_data_手术开始结束时间存在问题.append(df_data[name])

                elif name == '体温信息测量时间存在问题':
                    if df_data_体温信息测量时间存在问题.shape[0] == 0:
                        df_data_体温信息测量时间存在问题 = df_data[name]
                    else:
                        df_data_体温信息测量时间存在问题 = df_data_体温信息测量时间存在问题.append(df_data[name])

        writer = pd.ExcelWriter(excel_name_问题数据明细, engine='openpyxl')
        df_data_出院时间存在问题.to_excel(excel_writer=writer, sheet_name='出院时间存在问题数据',encoding='utf-8', index=False, header=True)
        df_data_医嘱开始结束时间存在问题.to_excel(excel_writer=writer, sheet_name='医嘱开始结束时间存在问题数据',encoding='utf-8', index=False, header=True)
        df_data_手术开始结束时间存在问题.to_excel(excel_writer=writer, sheet_name='手术开始结束时间存在问题数据',encoding='utf-8', index=False, header=True)
        df_data_体温信息测量时间存在问题.to_excel(excel_writer=writer, sheet_name='体温信息测量时间存在问题数据',encoding='utf-8', index=False, header=True)
        writer.save()
        writer.close()

        zipname = "./out/" + now_time + param.hospname + ".zip"
        f = zipfile.ZipFile(zipname, 'a', zipfile.ZIP_DEFLATED)
        f.write(csv_name_感染性事件, csv_name_感染性事件[csv_name_感染性事件.rfind('/') + 1:])
        f.write(csv_name_手术患者明细, csv_name_手术患者明细[csv_name_手术患者明细.rfind('/') + 1:])
        f.write(csv_name_非手术患者明细, csv_name_非手术患者明细[csv_name_非手术患者明细.rfind('/') + 1:])
        f.write(csv_name_医嘱明细, csv_name_医嘱明细[csv_name_医嘱明细.rfind('/') + 1:])
        f.write(excel_name_问题数据明细, excel_name_问题数据明细[excel_name_问题数据明细.rfind('/') + 1:])
        if os.path.exists(csv_name_未识别抗菌药物):
            f.write(csv_name_未识别抗菌药物, csv_name_未识别抗菌药物[csv_name_未识别抗菌药物.rfind('/') + 1:])
        f.close()
        lis_style[0] = html.Li(html.A(f'请点击下载计算结果：/{param.hospname + now_time + param.hospname + ".zip"}', href=f'/download/{now_time + param.hospname + ".zip"}', target='_blank'))

        df_nxraw = pd.read_csv(csv_name_感染性事件,encoding='utf-8')
        df_nxraw['入院时间'] = pd.to_datetime(df_nxraw['入院时间'].str[0:10])
        df_nxraw_new = df_nxraw.replace({'是': 1, '否': 0})
        df_nxraw_进行感染性事件计算 = df_nxraw_new[~df_nxraw_new['是否院内感染'].isnull()]
        a = list(df_nxraw_进行感染性事件计算['体温异常次数'].unique())
        a.sort()
        a.remove(0)
        df_nxraw_进行感染性事件计算['是否发热'] = df_nxraw_进行感染性事件计算['体温异常次数'].replace(a, 1)
        df_nxraw_进行计算_不同医院抗菌药发热比例 = df_nxraw_进行感染性事件计算.groupby(
            ['医院名称', df_nxraw_进行感染性事件计算['入院时间'].dt.year, df_nxraw_进行感染性事件计算['入院时间'].dt.month]).agg(
            {'caseid': 'count', '是否存在抗菌药物医嘱信息': 'sum', '是否发热': 'sum', '是否存在手术信息': 'sum'})
        df_nxraw_进行计算_不同医院抗菌药发热比例.columns = ['总人数', '有抗菌药物医嘱人数', '发热患者总数', '手术患者总数']
        df_nxraw_进行计算_不同医院抗菌药发热比例['抗菌药物医嘱比例'] = df_nxraw_进行计算_不同医院抗菌药发热比例['有抗菌药物医嘱人数'] / df_nxraw_进行计算_不同医院抗菌药发热比例[
            '总人数']
        df_nxraw_进行计算_不同医院抗菌药发热比例['发热患者比例'] = df_nxraw_进行计算_不同医院抗菌药发热比例['发热患者总数'] / df_nxraw_进行计算_不同医院抗菌药发热比例['总人数']
        df_nxraw_进行计算_不同医院抗菌药发热比例['手术患者比例'] = df_nxraw_进行计算_不同医院抗菌药发热比例['手术患者总数'] / df_nxraw_进行计算_不同医院抗菌药发热比例['总人数']
        df_nxraw_进行计算_不同医院抗菌药发热比例.sort_values(by='总人数', ascending=False)
        df_nxraw_进行计算_不同医院抗菌药发热比例.rename_axis(index=['医院名称', '入院年份', '入院月份'], inplace=True)
        df_nxraw_进行计算_不同医院抗菌药发热比例 = df_nxraw_进行计算_不同医院抗菌药发热比例.reset_index()
        df_nxraw_进行计算_不同医院抗菌药发热比例['月份'] = df_nxraw_进行计算_不同医院抗菌药发热比例.apply(
            lambda x: str(x.入院年份) + '.' + str(x.入院月份) if int(x.入院月份) >= 10 else str(x.入院年份) + '.0' + str(x.入院月份), axis=1)

        df_nxraw_进行计算_不同医院抗菌药发热比例 = df_nxraw_进行计算_不同医院抗菌药发热比例[['月份','总人数', '抗菌药物医嘱比例', '发热患者比例', '手术患者比例']]

        lis_style[-1] = df_nxraw_进行计算_不同医院抗菌药发热比例.to_json(date_format = 'iso', orient = 'split')
    return lis_style


@app.callback(
    Output('hos_line', 'figure'),
    Input('fig_type','value'),
    Input('intermediate-value','children'),
    prevent_initial_call=True
)
def update_graph(fig_type,jsonified_cleaned_data):
    try:
        df = pd.read_json(jsonified_cleaned_data, orient='split')
        print(df)
    except:
        return dash.no_update
    df = df[['月份',fig_type]]
    df_月份 = df[['月份']].drop_duplicates()
    df = df_月份.merge(df, on='月份', how='left')
    df = df.sort_values(['月份'])
    fig = px.line(df, x="月份", y=fig_type ,title = '计算结果展示' )
    return fig


@app.callback(
    Output("merge-date-down", "children"),
    Input("merge-date","n_clicks"),
    State('dash-uploader','upload_id'),
    State('dash-uploader','fileNames'),
    State('dash-uploader','isCompleted'),
    prevent_initial_call=True
)
def merge_date(nclicks,upload_id,filenames,isCompleted):
    print("nclicks:",nclicks)

    if not isCompleted:
        return dash.no_update
    rootdir = os.getcwd() + '\\upload' + '\\' + upload_id
    res_df = pd.DataFrame()
    for parent, dirnames, filenames in os.walk(rootdir):
        for filename in filenames:
            if os.path.exists(parent + '\\' + filename):
                if res_df.shape[0] == 0:
                    res_df = pd.read_csv(parent + '\\' + filename, encoding='utf-8')
                else:
                    res_df = res_df.append(pd.read_csv(parent + '\\' + filename, encoding='utf-8'))

    csv_name = str(uuid.uuid1()) + '合并结果'
    print("csv_name:", rootdir + '\\' + csv_name + '.csv')
    res_df.to_csv(rootdir + '\\' + csv_name + '.csv',encoding='utf-8')
    zipname = os.getcwd() + '\\upload'+'\\'+ csv_name +'.zip'
    f = zipfile.ZipFile(zipname, 'a', zipfile.ZIP_DEFLATED)
    f.write( rootdir + '\\' + csv_name+'.csv', csv_name+'.csv')
    f.close()
    print("zipname:",zipname)
    return html.Li(html.A(f'请点击下载合并结果：/{csv_name}.zip', href=f'/download1/{csv_name}.zip', target='_blank'))


# 图一结果计算
@app.callback(
    Output("intermediate-value1", "children"),

    Input("merge-date", "n_clicks"),
    State('dash-uploader', 'upload_id'),
    State('dash-uploader', 'isCompleted')
)
def intermediate_value1(nclicks, upload_id, isCompleted):
    if not isCompleted:
        return dash.no_update
    rootdir = os.getcwd() + '\\upload' + '\\' + upload_id
    res_df = pd.DataFrame()
    for parent, dirnames, filenames in os.walk(rootdir):
        for filename in filenames:
            if os.path.exists(parent + '\\' + filename):
                if res_df.shape[0] == 0:
                    res_df = pd.read_csv(parent + '\\' + filename, encoding='utf-8')
                else:
                    res_df = res_df.append(pd.read_csv(parent + '\\' + filename, encoding='utf-8'))
    res_df = res_df.drop_duplicates()
    if res_df.shape[0] > 0:
        res_df['入院时间'] = pd.to_datetime(res_df['入院时间'].str[0:10])
        df_nxraw_new = res_df.replace({'是': 1, '否': 0})

        df_nxraw_未进行感染性事件计算 = df_nxraw_new[df_nxraw_new['是否院内感染'].isnull()]
        df_nxraw_new_不同医院总人数 = df_nxraw_new.groupby(['医院名称']).agg({'caseid': 'count'}).reset_index()
        df_nxraw_不同医院计算缺失人数 = df_nxraw_未进行感染性事件计算.groupby(['医院名称']).agg({'caseid': 'count'}).reset_index()
        df_nxraw_未进行感染性事件计算比例 = pd.merge(df_nxraw_new_不同医院总人数, df_nxraw_不同医院计算缺失人数, on='医院名称', how='outer')
        df_nxraw_未进行感染性事件计算比例['未计算比例'] = df_nxraw_未进行感染性事件计算比例['caseid_y'] / df_nxraw_未进行感染性事件计算比例['caseid_x']
        df_nxraw_未进行感染性事件计算比例排序 = df_nxraw_未进行感染性事件计算比例.sort_values(by='未计算比例', ascending=False)
        df_nxraw_未进行感染性事件计算比例排序.columns = ['医院名称', '患者总数', '未进行计算人数', '未感染性事件计算比例']

        return df_nxraw_未进行感染性事件计算比例排序.to_json(date_format='iso', orient='split')
    else:
        return dash.no_update

# 图二结果计算
@app.callback(
    Output("intermediate-value2", "children"),
    Output("event_hospital","options"),
    Output("event_hospital","value"),

    Input("merge-date","n_clicks"),
    State('dash-uploader','upload_id'),
    State('dash-uploader','isCompleted')
)
def intermediate_value2(nclicks,upload_id,isCompleted):
    if not isCompleted:
        return dash.no_update
    rootdir = os.getcwd() + '\\upload' + '\\' + upload_id
    res_df = pd.DataFrame()
    for parent, dirnames, filenames in os.walk(rootdir):
        for filename in filenames:
            if os.path.exists(parent + '\\' + filename):
                if res_df.shape[0] == 0:
                    res_df = pd.read_csv(parent + '\\' + filename, encoding='utf-8')
                else:
                    res_df = res_df.append(pd.read_csv(parent + '\\' + filename, encoding='utf-8'))
    res_df = res_df.drop_duplicates()
    if res_df.shape[0]>0:
        res_df['入院时间'] = pd.to_datetime(res_df['入院时间'].str[0:10])

        df_nxraw_wjxgrxsj = res_df.groupby(['医院名称', '患者未进行感染性事件计算原因'])[['caseid']].count().reset_index()
        df_nxraw_yyrs = res_df.groupby(['医院名称'])[['caseid']].count().reset_index()
        ddf = df_nxraw_wjxgrxsj.merge(df_nxraw_yyrs, on='医院名称')
        ddf['value'] = ddf['caseid_x'] / ddf['caseid_y']
        ddf = ddf[['医院名称', '患者未进行感染性事件计算原因', 'value']]
        ddf.columns = ['医院名称', 'name', 'value']
        options =  [{'label': i, 'value': i} for i in ddf['医院名称'].drop_duplicates()]

        value = list(ddf['医院名称'].drop_duplicates())[0]
        return ddf.to_json(date_format='iso', orient='split'),options,value
    else:
        return dash.no_update


# 图三结果计算
@app.callback(
    Output("intermediate-value3", "children"),
    Output("lis_hos", "options"),
    Output("lis_hos", "value"),

    Input("merge-date", "n_clicks"),
    State('dash-uploader', 'upload_id'),
    State('dash-uploader', 'isCompleted')
)
def intermediate_value3(nclicks, upload_id, isCompleted):
    if not isCompleted:
        return dash.no_update
    rootdir = os.getcwd() + '\\upload' + '\\' + upload_id
    res_df = pd.DataFrame()
    for parent, dirnames, filenames in os.walk(rootdir):
        for filename in filenames:
            if os.path.exists(parent + '\\' + filename):
                if res_df.shape[0] == 0:
                    res_df = pd.read_csv(parent + '\\' + filename, encoding='utf-8')
                else:
                    res_df = res_df.append(pd.read_csv(parent + '\\' + filename, encoding='utf-8'))
    res_df = res_df.drop_duplicates()

    if res_df.shape[0] > 0:
        res_df['入院时间'] = pd.to_datetime(res_df['入院时间'].str[0:10])
        df_nxraw_new = res_df.replace({'是': 1, '否': 0})

        df_nxraw_进行感染性事件计算 = df_nxraw_new[~df_nxraw_new['是否院内感染'].isnull()]
        a = list(df_nxraw_进行感染性事件计算['体温异常次数'].unique())
        a.sort()
        a.remove(0)
        df_nxraw_进行感染性事件计算['是否发热'] = df_nxraw_进行感染性事件计算['体温异常次数'].replace(a, 1)
        df_nxraw_进行计算_不同医院抗菌药发热比例 = df_nxraw_进行感染性事件计算.groupby(
            ['医院名称', df_nxraw_进行感染性事件计算['入院时间'].dt.year, df_nxraw_进行感染性事件计算['入院时间'].dt.month]).agg(
            {'caseid': 'count', '是否存在抗菌药物医嘱信息': 'sum', '是否发热': 'sum', '是否存在手术信息': 'sum'})
        df_nxraw_进行计算_不同医院抗菌药发热比例.columns = ['总人数', '有抗菌药物医嘱人数', '发热患者总数', '手术患者总数']
        df_nxraw_进行计算_不同医院抗菌药发热比例['抗菌药物医嘱比例'] = df_nxraw_进行计算_不同医院抗菌药发热比例['有抗菌药物医嘱人数'] / df_nxraw_进行计算_不同医院抗菌药发热比例[
            '总人数']
        df_nxraw_进行计算_不同医院抗菌药发热比例['发热患者比例'] = df_nxraw_进行计算_不同医院抗菌药发热比例['发热患者总数'] / df_nxraw_进行计算_不同医院抗菌药发热比例['总人数']
        df_nxraw_进行计算_不同医院抗菌药发热比例['手术患者比例'] = df_nxraw_进行计算_不同医院抗菌药发热比例['手术患者总数'] / df_nxraw_进行计算_不同医院抗菌药发热比例['总人数']
        df_nxraw_进行计算_不同医院抗菌药发热比例.sort_values(by='总人数', ascending=False)
        df_nxraw_进行计算_不同医院抗菌药发热比例.rename_axis(index=['医院名称', '入院年份', '入院月份'], inplace=True)
        dddf = df_nxraw_进行计算_不同医院抗菌药发热比例.reset_index()
        dddf['月份'] = dddf.apply(
            lambda x: str(x.入院年份) + '.' + str(x.入院月份) if x.入院月份 >= 10 else str(x.入院年份) + '.0' + str(x.入院月份), axis=1)

        options = [{'label': i, 'value': i} for i in dddf['医院名称'].drop_duplicates()]
        value = list(dddf['医院名称'].drop_duplicates())[0]
        return dddf.to_json(date_format='iso', orient='split'), options, value
    else:
        return dash.no_update

@app.callback(
    dash.dependencies.Output('hos_bar', 'figure'),
    dash.dependencies.Input('event_type', 'value'),
    dash.dependencies.Input('intermediate-value1', 'children'),
    prevent_initial_call=True)
def update_graph(event_type,jsonified_cleaned_data ):
    try:
        df = pd.read_json(jsonified_cleaned_data, orient='split')
        df = df[['医院名称', event_type]]
        fig = px.bar(df, x="医院名称", y=event_type)
        return fig
    except:
        return dash.no_update


@app.callback(
    dash.dependencies.Output('hos_pie', 'figure'),
    dash.dependencies.Input('event_hospital', 'value'),
dash.dependencies.Input('intermediate-value2', 'children'),
    prevent_initial_call=True
)
def update_graph(event_hospital,jsonified_cleaned_data ):
    try:
        ddf = pd.read_json(jsonified_cleaned_data, orient='split')
        fig = px.pie(ddf[ddf['医院名称'] == event_hospital], values='value', names='name', title=event_hospital)
        return fig
    except:
        return dash.no_update


@app.callback(
    dash.dependencies.Output('hos_line_mul', 'figure'),
    [dash.dependencies.Input('lis_hos', 'value')],
    dash.dependencies.Input('fig_type', 'value'),
dash.dependencies.Input('intermediate-value3', 'children'),
    prevent_initial_call=True)
def update_graph(lis_hos,fig_type ,jsonified_cleaned_data):
    dddf = pd.read_json(jsonified_cleaned_data, orient='split')
    print(dddf,lis_hos,fig_type)
    lis = []
    if type(lis_hos) == type([]):
        lis = lis_hos
    else:
        lis.append(lis_hos)

    df = dddf[dddf['医院名称'].isin(lis)][['医院名称', '入院年份', '入院月份', '月份', fig_type]]
    df_月份 = df[['月份']].drop_duplicates()
    print(df_月份['月份'])
    df = df_月份.merge(df, on='月份', how='left')
    df = df.sort_values(['月份'])
    fig = px.line(df, x="月份", y=fig_type, color='医院名称')
    return fig

    # try:
    #     dddf = pd.read_json(jsonified_cleaned_data, orient='split')
    #     print(dddf)
    #     df = dddf[dddf['医院名称'].isin(lis_hos)][['医院名称', '入院年份', '入院月份','月份',fig_type]]
    #     df_月份 = df[['月份']].drop_duplicates()
    #     print(list(df_月份['月份']))
    #     df = df_月份.merge(df, on='月份', how='left')
    #     df = df.sort_values(['月份'])
    #     fig = px.line(df, x="月份", y=fig_type, color='医院名称')
    #     return fig
    # except:
    #     return dash.no_update





if __name__ == "__main__":
    app.run_server(debug=False,port=8009)
