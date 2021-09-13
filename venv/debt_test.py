import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint
from datetime import datetime
from pandas.tseries.offsets import DateOffset

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def debt():
    engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.8:3306/gfk", pool_pre_ping=True)
    conn = engine.connect()

    def add_quotes(q):
        return "'" + q + "'"

    clients = pd.DataFrame(pd.read_sql_query("select distinctrow ca.client_id from credit_application ca where ca.client_id in (1151365,	198142,	1268347,	1250588,	1312083,	1283646,	1312111,	1312125,	1312138,	1312140,	1300165,	1193740,	1287682,	1312160,	1312165,	944638,	1311471,	1312182,	1312197,	1312203);", conn))
    search_clients = []
    for client_id in clients.client_id:
        client_search = pd.DataFrame(pd.read_sql_query(" select lh.sum as debt, ca.client_id from loan_history lh join credit_application ca on lh.loan_id = ca.loan_id where lh.active_begin >= (ca.creation_date) and (ifnull(lh.active_end, lh.end_date) <= (DATE_ADD(ca.creation_date, INTERVAL 120 DAY )) or if(lh.active_end > (DATE_ADD(ca.creation_date, INTERVAL 120 DAY)), lh.end_date, (ifnull(lh.active_end, lh.end_date) <= (DATE_ADD(ca.creation_date, INTERVAL 120 DAY ))))) and ca.client_id = " + str(client_id) + " ORDER BY lh.id DESC LIMIT 1; ", conn))
        search_clients.append(client_search)

    result = pd.concat(search_clients)
    cols = result.columns.tolist()
    result = result[cols]
    result_df = pd.DataFrame(result)
    # print(result_df)
    result_df.to_excel("debt" " .xlsx", sheet_name="debt")


debt()

