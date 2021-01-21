import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint
from datetime import datetime

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def loan_approve_report():
    engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.8:3306/gfk", pool_pre_ping=True)
    conn = engine.connect()

    def add_quotes(q):
        return "'" + q + "'"

    date_1 = input("Введите дату от в формате YYYY-MM-DD: ")
    date_2 = input("Введите дату до в формате YYYY-MM-DD: ")

    total_apps_query = "select s.name, count(ca.id) as all_apps from credit_application ca join staff s on ca.staff_id = s.id where cnt_closed_loans = 0 and creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and s.name != 'Robot' group by s.name"
    loan_apps_query = "select s.name, count(ca.id) as loan_apps from credit_application ca join staff s on ca.staff_id = s.id where cnt_closed_loans = 0 and creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and s.name != 'Robot' and loan_id is not null group by s.name"

    total_apps = pd.read_sql_query(total_apps_query, conn)
    loan_apps = pd.read_sql_query(loan_apps_query, conn)

    t_a_df = pd.DataFrame(total_apps)
    l_a_df = pd.DataFrame(loan_apps)
    report = t_a_df.merge(l_a_df)

    report["approval %"] = round((report.loan_apps / report.all_apps) * 100, 0)

    # pprint(report)
    report.to_excel("loan_approval_stat " + add_quotes(date_1) + " - " + add_quotes(str(datetime.date(pd.to_datetime(date_2) - pd.offsets.Day(1)))) + " .xlsx", sheet_name="loan_stat")


loan_approve_report()
