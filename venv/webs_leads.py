import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint
from datetime import datetime
from pandas.tseries.offsets import DateOffset
import json
from pandas.io.json import json_normalize

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.8:3306/gfk", pool_pre_ping=True)
conn = engine.connect()


def add_quotes(q):
    return "'" + q + "'"


# def lead_click():
#     lead_data = pd.DataFrame(pd.read_sql_query(
#         "select lc.lead_data, lc.created_at from lead_click lc where lc.created_at between '2021-07-01' and '2021-08-01'",
#         conn))
#
#     lead_data['guruleads'] = lead_data['lead_data'].str.contains('guruleads', case=False)
#     lead_data['leadssu'] = lead_data['lead_data'].str.contains('leadssu', case=False)
#     lead_data['rafinad'] = lead_data['lead_data'].str.contains('rafinad', case=False)
#     lead_data['advertise'] = lead_data['lead_data'].str.contains('advertise', case=False)
#     lead_data['leadtarget'] = lead_data['lead_data'].str.contains('leadtarget', case=False)
#     lead_data['regbet'] = lead_data['lead_data'].str.contains('regbet', case=False)
#     lead_data['unicom24'] = lead_data['lead_data'].str.contains('unicom24', case=False)
#     lead_data['beegl'] = lead_data['lead_data'].str.contains('beegl', case=False)
#     lead_data['leadcraft'] = lead_data['lead_data'].str.contains('leadcraft', case=False)
#     lead_data['linkprofit'] = lead_data['lead_data'].str.contains('linkprofit', case=False)
#     lead_data['cityads'] = lead_data['lead_data'].str.contains('cityads', case=False)
#     lead_data['finpublic'] = lead_data['lead_data'].str.contains('finpublic', case=False)
#     lead_data['gidfinance'] = lead_data['lead_data'].str.contains('gidfinance', case=False)
#     lead_data['liknot'] = lead_data['lead_data'].str.contains('liknot', case=False)
#     lead_data['leadgid'] = lead_data['lead_data'].str.contains('leadgid', case=False)
#     lead_data['bankiros'] = lead_data['lead_data'].str.contains('bankiros', case=False)
#     lead_data['teleport'] = lead_data['lead_data'].str.contains('teleport', case=False)
#     lead_data['actionpay'] = lead_data['lead_data'].str.contains('actionpay', case=False)
#     lead_data['bankiru'] = lead_data['lead_data'].str.contains('bankiru', case=False)
#     lead_data['saleads'] = lead_data['lead_data'].str.contains('saleads', case=False)
#     lead_data['leadstech'] = lead_data['lead_data'].str.contains('leadstech', case=False)
#     lead_data['yandexrsya'] = lead_data['lead_data'].str.contains('yandexrsya', case=False)
#     lead_data['unicom24'] = lead_data['lead_data'].str.contains('unicom24', case=False)
#     lead_data['yandexsearch'] = lead_data['lead_data'].str.contains('yandexsearch', case=False)
#     lead_data['yandexbrand'] = lead_data['lead_data'].str.contains('yandexbrand', case=False)
#
#     lead_data.loc[lead_data['guruleads'] == True, 'lead_data'] = 'guruleads'
#     lead_data.loc[lead_data['leadssu'] == True, 'lead_data'] = 'leadssu'
#     lead_data.loc[lead_data['rafinad'] == True, 'lead_data'] = 'rafinad'
#     lead_data.loc[lead_data['advertise'] == True, 'lead_data'] = 'advertise'
#     lead_data.loc[lead_data['leadtarget'] == True, 'lead_data'] = 'leadtarget'
#     lead_data.loc[lead_data['regbet'] == True, 'lead_data'] = 'regbet'
#     lead_data.loc[lead_data['unicom24'] == True, 'lead_data'] = 'unicom24'
#     lead_data.loc[lead_data['beegl'] == True, 'lead_data'] = 'beegl'
#     lead_data.loc[lead_data['leadcraft'] == True, 'lead_data'] = 'leadcraft'
#     lead_data.loc[lead_data['linkprofit'] == True, 'lead_data'] = 'linkprofit'
#     lead_data.loc[lead_data['cityads'] == True, 'lead_data'] = 'cityads'
#     lead_data.loc[lead_data['finpublic'] == True, 'lead_data'] = 'finpublic'
#     lead_data.loc[lead_data['gidfinance'] == True, 'lead_data'] = 'gidfinance'
#     lead_data.loc[lead_data['liknot'] == True, 'lead_data'] = 'liknot'
#     lead_data.loc[lead_data['leadgid'] == True, 'lead_data'] = 'leadgid'
#     lead_data.loc[lead_data['bankiros'] == True, 'lead_data'] = 'bankiros'
#     lead_data.loc[lead_data['teleport'] == True, 'lead_data'] = 'teleport'
#     lead_data.loc[lead_data['actionpay'] == True, 'lead_data'] = 'actionpay'
#     lead_data.loc[lead_data['bankiru'] == True, 'lead_data'] = 'bankiru'
#     lead_data.loc[lead_data['saleads'] == True, 'lead_data'] = 'saleads'
#     lead_data.loc[lead_data['leadstech'] == True, 'lead_data'] = 'leadstech'
#     lead_data.loc[lead_data['yandexrsya'] == True, 'lead_data'] = 'yandexrsya'
#     lead_data.loc[lead_data['unicom24'] == True, 'lead_data'] = 'unicom24'
#     lead_data.loc[lead_data['yandexsearch'] == True, 'lead_data'] = 'yandexsearch'
#     lead_data.loc[lead_data['yandexbrand'] == True, 'lead_data'] = 'yandexbrand'
#
#     final_lead = lead_data.drop([
#         'guruleads', 'leadssu', 'rafinad', 'advertise', 'leadtarget', 'regbet', 'unicom24', 'beegl', 'leadcraft',
#         'linkprofit', 'cityads', 'finpublic', 'gidfinance', 'liknot', 'leadgid', 'bankiros', 'teleport', 'actionpay',
#         'bankiru', 'saleads', 'leadstech', 'yandexrsya', 'unicom24', 'yandexsearch', 'yandexbrand'], axis=1)
#
#     print(final_lead)
#     final_lead.to_excel("lead_data "".xlsx", sheet_name="ltv")


# lead_click()


def web_id():
    global result_debt
    data = pd.DataFrame(pd.read_sql_query(
        "select ld.lead_data, ld.created_at from lead_click ld where ld.created_at between '2021-07-01' and '2021-08-01'", conn))

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

    utm_wm_data = utm_wm_data.replace({'wm': {None: '0'}})
    utm_wm_data['lead_wm'] = utm_wm_data['utm'].str.cat(utm_wm_data['wm'], sep="_")
    utm_wm_data_count = utm_wm_data.groupby(['lead_wm'], as_index=False).count()
    final_count = utm_wm_data_count.drop(['created_at', 'utm'], axis=1)

    apps_loans_data = pd.DataFrame(pd.read_sql_query("select concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0)) as lead_wm, count(concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0))) as cnt_app, ifnull((select count(concat_ws('_',ca2.lead_provider, coalesce(ca2.wm_id, 0))) from credit_application ca2 where ca2.loan_id is not null and ca2.creation_date between '2021-07-01' and '2021-08-01' and ca2.lead_provider is not null and concat_ws('_',ca2.lead_provider, coalesce(ca2.wm_id, 0)) = concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0)) group by concat_ws('_',ca2.lead_provider, coalesce(ca2.wm_id, 0))), 0) as cnt_loan from credit_application ca where ca.creation_date between '2021-07-01' and '2021-08-01' and ca.lead_provider is not null group by concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0));", conn))

    source_data = final_count.join(apps_loans_data.set_index('lead_wm'), on='lead_wm')
    source_data = source_data.rename(columns={'wm': 'clicks'})
    source_data = source_data.replace({'cnt_app': {None: '0'}})
    source_data = source_data.replace({'cnt_loan': {None: '0'}})
    source_data['CTR%'] = round(source_data['cnt_app'].astype(int) / source_data['clicks'].astype(int) * 100, 2)
    source_data['AR%'] = round(source_data['cnt_loan'].astype(int) / source_data['clicks'].astype(int) * 100, 2)

    npl_df = pd.DataFrame(pd.read_sql_query("select concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0)) as lead_wm, sum(if((select distinctrow lh.loan_id from loan_history lh join loan_retro_stat lrs on lh.loan_id = lrs.loan_id where lrs.date = (select date_add(min(lh2.end_date), interval 4 day ) from loan_history lh2 where lh2.loan_id = lrs.loan_id) and lh.loan_id = ca.loan_id and overdue_days > 3 and remaining_total > 0 ) > 0, 1, 0)) as 'NPL_3' from credit_application ca where ca.creation_date between '2021-07-01' and '2021-08-01' and ca.loan_id is not null and ca.lead_provider is not null group by concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0))", conn))

    source_data = source_data.join(npl_df.set_index('lead_wm'), on='lead_wm')
    source_data = source_data.replace({'NPL_3': {None: '0'}})
    source_data['NPL_3%'] = round(source_data['NPL_3'].astype(int) / source_data['cnt_loan'].astype(int) * 100, 2)
    source_data = source_data.replace({'NPL_3%': {None: '0'}})
    source_data = source_data.reindex(columns=['lead_wm', 'clicks', 'cnt_app', 'cnt_loan', 'NPL_3', 'CTR%', 'AR%', 'NPL_3%'])

    clients_df = (pd.read_sql_query("select ca.client_id from credit_application ca where ca.loan_id is not null and ca.creation_date between '2021-07-01' and '2021-08-01' and ca.lead_provider is not null", conn))

    search_clients = []
    for client_id in clients_df.client_id:
        client_search = pd.DataFrame(pd.read_sql_query("select lh.sum as debt, ca.client_id from loan_history lh join credit_application ca on lh.loan_id = ca.loan_id where lh.active_begin >= (ca.creation_date) and ifnull(lh.active_end, lh.end_date) <= (DATE_ADD(ca.creation_date, INTERVAL 120 DAY )) and creation_date between '2021-07-01' and '2021-08-01' and ca.client_id = " + str(client_id) + " order by (case lh.id when ifnull(lh.active_end, lh.end_date) <= (date_add(ca.creation_date, interval 120 day )) then lh.id end) desc, (case lh.id when ifnull(lh.active_end, lh.end_date) <= (date_add(ca.creation_date, interval 120 day )) then lh.id end) limit 1;", conn))
        search_clients.append(client_search)

        result_debt = pd.concat(search_clients)
        cols_deb = result_debt.columns.tolist()
        result_debt = result_debt[cols_deb]

    ltv_df = (pd.read_sql_query("select ca.client_id, concat_ws('_',ca.lead_provider, coalesce(ca.wm_id, 0)) as lead_wm, sum((select sum(sum) from incoming_transfer it where l.id = it.loan_id and it.destination != 'body' and ca.cnt_closed_loans = 0 and (it.creation_date) between ca.creation_date and (DATE_ADD(ca.creation_date, INTERVAL 120 DAY)))) as sum_120 from credit_application ca join loan l on ca.client_id = l.client_id where ca.client_id in (select ca2.client_id from credit_application ca2 where ca2.loan_id is not null and ca2.creation_date between '2021-07-01' and '2021-08-01' and ca2.lead_provider is not null) and ca.lead_provider is not null group by ca.client_id order by ca.creation_date;", conn))

    pnl_df = pd.merge(ltv_df, result_debt, on=['client_id'])
    pnl_df = pnl_df.replace({'sum_120': {None: 0}})
    pnl_df['LTV_120'] = pnl_df['sum_120'] - pnl_df['debt']
    pnl_df = pnl_df.drop(['client_id', 'sum_120', 'debt'], axis=1)
    pnl_df = pnl_df.groupby(['lead_wm'], as_index=False).sum()

    source_data = source_data.join(pnl_df.set_index('lead_wm'), on='lead_wm')
    source_data = source_data.replace({'LTV_120': {None: '0'}})

    source_data.loc[source_data['cnt_loan'].astype(int) == 0, 'LTV_120'] = 0
    source_data['LTV_120_avg'] = round(source_data['LTV_120'].astype(int) / source_data['cnt_loan'].astype(int), 2)
    source_data = source_data.replace({'LTV_120_avg': {None: '0'}})

    source_data = source_data.drop(['LTV_120'], axis=1)

    source_data['clicks'] = source_data['clicks'].astype(int)
    source_data['cnt_app'] = source_data['cnt_app'].astype(int)
    source_data['cnt_loan'] = source_data['cnt_loan'].astype(int)
    source_data['cnt_loan'] = source_data['cnt_loan'].astype(int)
    source_data['NPL_3'] = source_data['NPL_3'].astype(int)
    source_data['LTV_120_avg'] = source_data['LTV_120_avg'].astype(int)
    source_data['CTR%'] = source_data['CTR%'].astype(int)
    source_data['AR%'] = source_data['AR%'].astype(int)
    source_data['NPL_3%'] = source_data['NPL_3%'].astype(int)

    source_data = source_data.reindex(columns=['lead_wm', 'clicks', 'cnt_app', 'cnt_loan', 'NPL_3', 'LTV_120_avg', 'CTR%', 'AR%', 'NPL_3%'])
    source_data = source_data.rename(columns={'CTR': 'CR'})
    source_data = source_data.rename(columns={'CTR%': 'CR%'})

    # source_data.to_excel("report_leads"".xlsx", sheet_name="board")
    # print(source_data)


web_id()
