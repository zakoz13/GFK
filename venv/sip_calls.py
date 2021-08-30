import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint
from datetime import datetime

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.170:3306/gfk", pool_pre_ping=True)
conn = engine.connect()


def add_quotes(q):
    return "'" + q + "'"

# def connect_date:
#     sip_calls_query = "select client_id, direction, type, started, answered, connected, ended, overdue, cause,description, phone from gfk_sip.calls where cause is not null and started between '2021-02-01' and '2021-02-02' order by client_id, started"
#     sip_calls = pd.read_sql_query(sip_calls_query, conn)
#     sip_calls_df = pd.DataFrame(sip_calls)
#
#     sip_calls_df['connected_date'] = sip_calls_df['connected'].astype('string')
#     conn_date_not_null = sip_calls_df[sip_calls_df['connected_date'] != '0000-00-00 00:00:00']
#     # client_not_null = pd.DataFrame(conn_date_not_null['client_id'].drop_duplicates())
#     list_id = [conn_date_not_null]
#     df_filter = pd.DataFrame(sip_calls_df['client_id'].isin([list_id]))
#     # df_filter.columns = ['client_id']
#     print(list_id)
#
#     # result = pd.merge_asof(sip_calls_df, df_filter)
#         # sip_calls_df.merge(df_filter)
#         # pd.concat([sip_calls_df, df_filter], join="inner")


# for element in sip_calls_df:
#     if element['connected_date'][0] != '0000-00-00 00:00:00':
#         result[element['client_id']] = element


def days_stat():

    date_1 = input("Введите дату начала мониторинга в формате YYYY-MM-DD: ")
    interval = int(input("interval "))
    # stat_sip = pd.DataFrame(pd.read_sql_query("select description, count(id) as cnt, started from gfk_sip.calls where started between " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d))))) + " and " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d + 1))))) + " and description != 'Unknown' group by description union select 'total_day', count(description), started from gfk_sip.calls where started between " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d))))) + " and " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d + 1))))) + " and description != 'Unknown'",conn))
    dfs = []
    for d in range(0, interval):
        stat_sip = pd.DataFrame(pd.read_sql_query("select description, count(id) as cnt, started from gfk_sip.calls where started between " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d))))) + " and " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d+1))))) + " and description != 'Unknown' group by description union select 'total_day', count(description), started from gfk_sip.calls where started between " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d))))) + " and " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d+1))))) + " and description != 'Unknown'", conn))
        d += 1
        dfs.append(stat_sip)
        # print(stat_sip)
        # stat_sip.to_excel("call_stat " + add_quotes(date_1) + " - " + str(interval) + ".xlsx", sheet_name="call_stat")

    result = pd.concat(dfs)
    print(result)
    result.to_excel("call_stat " + add_quotes(date_1) + " - " + str(interval) + ".xlsx", sheet_name="call_stat")


def hours_stat():

    date_1 = input("Введите дату начала мониторинга в формате YYYY-MM-DD: ")
    interval = int(input("interval "))
    dfs = []
    for d in range(0, interval):
        hour_sip = pd.DataFrame(pd.read_sql_query("select date(started), count((hour(started))) from gfk_sip.calls where started between " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d))))) + " and " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d + 1))))) + " group by (date(started)) union select hour(started), count(hour(started)) from gfk_sip.calls where started between " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d))))) + " and " + (add_quotes(str(datetime.date(pd.to_datetime(date_1) + pd.offsets.Day(d + 1))))) + " group by (hour(started))", conn))
        d += 1
        dfs.append(hour_sip)

    result = pd.concat(dfs)
    # print(result)
    result.to_excel("hour_stat " + add_quotes(date_1) + " - " + str(interval) + ".xlsx", sheet_name="hour_stat")


# days_stat()
hours_stat()
