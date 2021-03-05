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


def ltv():
    engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.170:3306/gfk", pool_pre_ping=True)
    conn = engine.connect()

    def add_quotes(q):
        return "'" + q + "'"

    date_1 = input("Введите дату начала жизни клиента в формате YYYY-MM-DD: ")

    # noinspection SqlAggregates,PyArgumentList
    ltv_df = pd.DataFrame(pd.read_sql_query("select ca.client_id, (select sum from loan_history lh where active_end is null and lh.loan_id = max(ca.loan_id)) as debt, sum((select sum(sum) from incoming_transfer it where ca.loan_id = it.loan_id and it.destination != 'body' and date(it.creation_date) between ca.creation_date and (date_add(ca.creation_date, INTERVAL 90 DAY)))) as sum_90, sum((select sum(sum) from incoming_transfer it where ca.loan_id = it.loan_id and it.destination != 'body' and date(it.creation_date) between ca.creation_date and (date_add(ca.creation_date, INTERVAL 180 DAY)))) as sum_180, sum((select sum(sum) from incoming_transfer it where ca.loan_id = it.loan_id and it.destination != 'body' and date(it.creation_date) between ca.creation_date and (date_add(ca.creation_date, INTERVAL 270 DAY)))) as sum_270, sum((select sum(sum) from incoming_transfer it where ca.loan_id = it.loan_id and it.destination != 'body' and date(it.creation_date) between ca.creation_date and (date_add(ca.creation_date, INTERVAL 360 DAY)))) as sum_360, (select total_overdue from loan l where l.id = max(ca.loan_id)) as overdue from credit_application ca where ca.client_id in (select c.id from client c where date(c.register_date) between " + add_quotes(date_1) + " and " + add_quotes(str(datetime.date(pd.to_datetime(date_1) + DateOffset(months=1)))) + ") and date(ca.creation_date) between " + add_quotes(date_1) + " and " + add_quotes(str(datetime.date(pd.to_datetime(date_1) + DateOffset(months=1)))) + " and ca.loan_id is not null group by ca.client_id order by ca.client_id", conn))
    ltv_df.fillna(0, inplace=True)
    ltv_df["PNL"] = ltv_df["sum_180"] - ltv_df["debt"]

    ltv_df["category"] = 0
    ltv_df.loc[ltv_df["PNL"] >= 30000, "category"] = "30+"
    ltv_df.loc[(ltv_df["PNL"] >= 20000) & (ltv_df["PNL"] <= 29999), "category"] = "20-29.9"
    ltv_df.loc[(ltv_df["PNL"] >= 10000) & (ltv_df["PNL"] <= 19999), "category"] = "10-19.9"
    ltv_df.loc[(ltv_df["PNL"] >= 6000) & (ltv_df["PNL"] <= 9999), "category"] = "6-9.9"
    ltv_df.loc[(ltv_df["PNL"] >= 3000) & (ltv_df["PNL"] <= 5999), "category"] = "3-5.9"
    ltv_df.loc[(ltv_df["PNL"] >= 0) & (ltv_df["PNL"] <= 2999), "category"] = "0-2.9"
    ltv_df.loc[ltv_df["PNL"] < 0, "category"] = "-0"

    ltv_df["client_date"] = date_1

    # print(ltv_df)
    ltv_df.to_excel("loan date " + add_quotes(date_1) + ".xlsx", sheet_name="ltv " + str(date_1))


ltv()
