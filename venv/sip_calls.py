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

sip_calls_query = "select client_id, direction, type, started, answered, connected, ended, overdue, cause,description, phone from gfk_sip.calls where cause is not null and started between '2021-02-01' and '2021-02-02' order by client_id, started"
sip_calls = pd.read_sql_query(sip_calls_query, conn)
sip_calls_df = pd.DataFrame(sip_calls)

sip_calls_df['connected_date'] = sip_calls_df['connected'].astype('string')
conn_date_not_null = sip_calls_df[sip_calls_df['connected_date'] != '0000-00-00 00:00:00']
# client_not_null = pd.DataFrame(conn_date_not_null['client_id'].drop_duplicates())
list_id = [conn_date_not_null]
df_filter = pd.DataFrame(sip_calls_df['client_id'].isin([list_id]))
# df_filter.columns = ['client_id']
print(list_id)


# result = pd.merge_asof(sip_calls_df, df_filter)
    # sip_calls_df.merge(df_filter)
    # pd.concat([sip_calls_df, df_filter], join="inner")










# for element in sip_calls_df:
#     if element['connected_date'][0] != '0000-00-00 00:00:00':
#         result[element['client_id']] = element










