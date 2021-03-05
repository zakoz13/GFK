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


def conversion():
    engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.170:3306/gfk", pool_pre_ping=True)
    conn = engine.connect()

    def add_quotes(q):
        return "'" + q + "'"

    date_1 = input("Введите дату от в формате YYYY-MM-DD: ")
    date_2 = input("Введите дату до в формате YYYY-MM-DD: ")
    n = int(input("Введите период конверсии(месяцев):  "))

    conversion_df = pd.DataFrame(pd.read_sql_query("select count(c.id) as first_loans, (select count(c.id) as second_loans from client c join credit_application ca on c.id = ca.client_id join loan l on ca.loan_id = l.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(str(datetime.date(pd.to_datetime(date_2) + DateOffset(months=n)))) + " and ca.cnt_closed_loans = 1 and c.id in (select c.id from client c join credit_application ca on c.id = ca.client_id join loan l on ca.loan_id = l.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.cnt_closed_loans = 0)) as second_loans, (select count(c.id) as third_loans from client c join credit_application ca on c.id = ca.client_id join loan l on ca.loan_id = l.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(str(datetime.date(pd.to_datetime(date_2) + DateOffset(months=(n*2))))) + " and ca.cnt_closed_loans = 2 and c.id in (select c.id from client c join credit_application ca on c.id = ca.client_id join loan l on ca.loan_id = l.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(str(datetime.date(pd.to_datetime(date_2) + DateOffset(months=n)))) + " and l.close_date is not null and ca.cnt_closed_loans = 1 and c.id in (select c.id from client c join credit_application ca on c.id = ca.client_id join loan l on ca.loan_id = l.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.cnt_closed_loans = 0))) as third_loans from client c join credit_application ca on c.id = ca.client_id join loan l on ca.loan_id = l.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.cnt_closed_loans = 0;", conn))
    conversion_df["% second from first"] = round((conversion_df["second_loans"] / conversion_df["first_loans"])*100, 0)
    conversion_df["% third from second"] = round((conversion_df["third_loans"] / conversion_df["second_loans"]) * 100, 0)
    conversion_df["% third from first"] = round((conversion_df["third_loans"] / conversion_df["first_loans"]) * 100, 0)

    conversion_df.to_excel("first loan date " + add_quotes(date_1) + " - " + add_quotes(str(datetime.date(pd.to_datetime(date_2) - pd.offsets.Day(1)))) + " conversion month  =" + str(n) + " .xlsx", sheet_name="clients")

    # print(add_quotes(str(datetime.date(pd.to_datetime(date_2) + DateOffset(months=n)))))
    # print(add_quotes(str(datetime.date(pd.to_datetime(date_2) + DateOffset(months=n*2)))))
    # print(conversion_df)


conversion()
