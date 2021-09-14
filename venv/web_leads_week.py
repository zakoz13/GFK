import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint
from datetime import datetime
from pandas.tseries.offsets import DateOffset
import json
from pandas.io.json import json_normalize
import numpy as np
# import seaborn as sns
# import matplotlib.pyplot as plt

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.8:3306/gfk", pool_pre_ping=True)
conn = engine.connect()


def add_quotes(q):
    return "'" + q + "'"


def web_id():

    date_1 = input("Введите дату начала мониторинга в формате YYYY-MM-DD: ")
    date_2 = input("Введите дату окончания мониторинга  в формате YYYY-MM-DD: ")

    data = pd.DataFrame(pd.read_sql_query("select ld.lead_data, ld.created_at from lead_click ld where ld.created_at between " + add_quotes(date_1) + " and " + add_quotes(date_2) + "", conn))

    lead_data = data['lead_data']
    lead_data = lead_data.dropna()
    data['utm'] = 0
    data['wm'] = 0

    ldid = []

    for lead in lead_data:
        dict_lead = json.loads(lead)  # словарь данных лид провайдера
        current_utm = dict_lead.get('utm_source')
        ldid.append(current_utm)

    ldseries = pd.Series(ldid)
    data['utm'] = ldseries

    wmid = []

    for wm in lead_data:
        dict_lead = json.loads(wm)  # словарь данных wm_id
        current_wm = dict_lead.get('wm_id')
        wmid.append(current_wm)

    wmseries = pd.Series(wmid)
    data['wm'] = wmseries

    utm_wm_data = data.drop(['lead_data'], axis=1)

    utm_wm_data = utm_wm_data.replace({'utm': {'leadssu': 'leadssu1'}})

    utm_wm_data = utm_wm_data.replace({'wm': {None: '0'}})

    utm_wm_data['lead_wm'] = utm_wm_data['utm'].str.cat(utm_wm_data['wm'], sep="_")

    utm_wm_data_count = utm_wm_data.groupby(['lead_wm'], as_index=False).count()
    final_count = utm_wm_data_count.drop(['created_at', 'utm'], axis=1)

    # apps_loans_data = pd.DataFrame(pd.read_sql_query("select concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0)) as lead_wm, count(concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0))) as cnt_app, ifnull((select count(concat_ws('_',ca2.lead_provider, coalesce(ca2.wm_id, 0))) from credit_application ca2 where ca2.loan_id is not null and ca2.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca2.lead_provider is not null and concat_ws('_',ca2.lead_provider, coalesce(ca2.wm_id, 0)) = concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0)) group by concat_ws('_',ca2.lead_provider, coalesce(ca2.wm_id, 0))), 0) as cnt_loan from credit_application ca where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.lead_provider is not null group by concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0));", conn))
    apps_loans_data = pd.DataFrame(pd.read_sql_query("select ca.lead_provider, ca.wm_id, count(concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0))) as cnt_app, ifnull((select count(concat_ws('_',ca2.lead_provider, coalesce(ca2.wm_id, 0))) from credit_application ca2 where ca2.loan_id is not null and ca2.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca2.lead_provider is not null and concat_ws('_',ca2.lead_provider, coalesce(ca2.wm_id, 0)) = concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0)) group by concat_ws('_',ca2.lead_provider, coalesce(ca2.wm_id, 0))), 0) as cnt_loan from credit_application ca where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.lead_provider is not null group by concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0));", conn))
    apps_loans_data.loc[apps_loans_data['lead_provider'] == 'saleads.pro', 'wm_id'] = 0
    apps_loans_data = apps_loans_data.replace({'wm_id': {None: '0'}})
    apps_loans_data['wm_id'] = apps_loans_data['wm_id'].astype(str)
    apps_loans_data['lead_wm'] = apps_loans_data['lead_provider'].str.cat(apps_loans_data['wm_id'], sep="_")
    apps_loans_data = apps_loans_data.groupby('lead_wm', as_index=False).sum()
    # apps_loans_data = apps_loans_data.drop(['lead_provider', 'wm_id'], axis=1)

    source_data = final_count.join(apps_loans_data.set_index('lead_wm'), on='lead_wm')
    source_data = source_data.rename(columns={'wm': 'clicks'})
    source_data = source_data.replace({'cnt_app': {None: '0'}})
    source_data = source_data.replace({'cnt_loan': {None: '0'}})
    source_data['CTR%'] = round(source_data['cnt_app'].astype(int) / source_data['clicks'].astype(int) * 100, 2)
    source_data['AR_app%'] = round(source_data['cnt_loan'].astype(int) / source_data['cnt_app'].astype(int) * 100, 2)
    source_data['AR_click%'] = round(source_data['cnt_loan'].astype(int) / source_data['clicks'].astype(int) * 100, 2)

    bad_loans = pd.DataFrame(pd.read_sql_query("select concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0)) as lead_wm, count(concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0))) as cnt_bad_loans from credit_application ca join loan l on ca.loan_id = l.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and l.client_id in (SELECT l2.client_id FROM loan l2 GROUP By l2.client_id having(COUNT(l2.id) = 1)) and date_add(date(l.issue_date), interval 3 day) = date(l.close_date) group by concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0));", conn))
    source_data = source_data.join(bad_loans.set_index('lead_wm'), on='lead_wm')
    source_data = source_data.replace({'cnt_bad_loans': {None: '0'}})
    source_data['bad_loans%'] = round(source_data['cnt_bad_loans'].astype(int) / source_data['cnt_loan'].astype(int) * 100, 2)
    source_data = source_data.replace({'AR_click%': {None: '0'}})
    source_data = source_data.replace({'AR_app%': {None: '0'}})
    source_data = source_data.replace({'bad_loans%': {None: '0'}})

    source_data['clicks'] = source_data['clicks'].astype(int)
    source_data['cnt_app'] = source_data['cnt_app'].astype(int)
    source_data['cnt_loan'] = source_data['cnt_loan'].astype(int)
    source_data['cnt_loan'] = source_data['cnt_loan'].astype(int)
    source_data['CTR%'] = source_data['CTR%'].astype(float)
    source_data['cnt_bad_loans'] = source_data['cnt_bad_loans'].astype(int)
    source_data['AR_app%'] = source_data['AR_app%'].astype(float)
    source_data['AR_click%'] = source_data['AR_click%'].astype(float)
    source_data['bad_loans%'] = source_data['bad_loans%'].astype(float)

    source_data = source_data.reindex(columns=['lead_wm', 'clicks', 'cnt_app', 'cnt_loan', 'cnt_bad_loans', 'CTR%', 'AR_click%', 'AR_app%', 'bad_loans%'])
    source_data = source_data.rename(columns={'CTR': 'CR'})
    source_data = source_data.rename(columns={'CTR%': 'CR%'})

    split_lead = source_data['lead_wm'].str.split('_', expand=True)
    col_list = len(split_lead.columns)

    if col_list == 4:
        split_lead.columns = ['lead', 'wm', '2', '3']
        split_lead = split_lead.drop(['2', '3'], axis=1)

    elif col_list == 2:
        split_lead.columns = ['lead', 'wm']

    source_data = pd.concat([source_data, split_lead], axis=1)

    source_data = source_data.reindex(columns=['lead', 'wm', 'lead_wm', 'clicks', 'cnt_app', 'cnt_loan', 'cnt_bad_loans', 'CR%', 'AR_click%', 'AR_app%', 'bad_loans%'])
    source_data = source_data.sort_values(by=['lead', 'clicks'], ascending=[True, False]).reset_index(drop=True)   # сортировка
    source_data['Date_1'] = date_1
    source_data['Date_2'] = date_2
    source_data['Date'] = source_data['Date_1'].astype(str) + ' - ' + source_data['Date_2'].astype(str)
    source_data = source_data.drop(['Date_1', 'Date_2'], axis=1)

    lead_sum = source_data.groupby('lead', as_index=False).sum()

    lead_sum['CR%'] = round(lead_sum['cnt_app'].astype(int) / lead_sum['clicks'].astype(int) * 100, 2)
    lead_sum['AR_app%'] = round(lead_sum['cnt_loan'].astype(int) / lead_sum['cnt_app'].astype(int) * 100, 2)
    lead_sum['AR_click%'] = round(lead_sum['cnt_loan'].astype(int) / lead_sum['clicks'].astype(int) * 100, 2)
    lead_sum['bad_loans%'] = round(lead_sum['cnt_bad_loans'].astype(int) / lead_sum['cnt_loan'].astype(int) * 100, 2)
    lead_sum = lead_sum.replace({'bad_loans%': {None: '0'}})
    lead_sum = lead_sum.replace({'AR_app%': {None: '0'}})
    lead_sum = lead_sum.replace({'bad_loans%': {None: '0'}})

    source_data = source_data.drop(['lead', 'wm'], axis=1)

    lead_sum['CR%'] = lead_sum['CR%'].astype(float)
    lead_sum['AR_app%'] = lead_sum['AR_app%'].astype(float)
    lead_sum['AR_click%'] = lead_sum['AR_click%'].astype(float)
    lead_sum['bad_loans%'] = lead_sum['bad_loans%'].astype(float)
    lead_sum = lead_sum.sort_values(by=['clicks'], ascending=[False]).reset_index(drop=True)
    lead_sum['Date_1'] = date_1
    lead_sum['Date_2'] = date_2
    lead_sum['Date'] = lead_sum['Date_1'].astype(str) + ' - ' + lead_sum['Date_2'].astype(str)
    lead_sum = lead_sum.drop(['Date_1', 'Date_2'], axis=1)

    # numeric_columns = ['clicks', 'cnt_app', 'cnt_loan', 'cnt_bad_loans', 'CR%', 'AR_click%', 'AR_app%', 'bad_loans%']
    # lead_sum.style.format('{:.1f}', na_rep='-').format({'lead': lambda x: x.lower()}).highlight_null(null_color='lightgrey').highlight_max(color='yellowgreen', subset=numeric_columns).highlight_min(color='coral', subset=numeric_columns)

    # print(lead_sum)
    # print(source_data)

    writer = pd.ExcelWriter("leads_wm_WEEK " + add_quotes(date_1) + " - " + add_quotes(str(datetime.date(pd.to_datetime(date_2) - pd.offsets.Day(1)))) + ".xlsx")
    lead_sum.to_excel(writer, "lead_sum")
    source_data.to_excel(writer, "lead_wm_report")
    writer.save()


web_id()
