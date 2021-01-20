import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)



def loan_report():
    engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.8:3306/gfk", pool_pre_ping=True)
    conn = engine.connect()

    date_1 = input("Введите дату от в формате YYYY-MM-DD: ")
    date_2 = input("Введите дату до в формате YYYY-MM-DD: ")
    date_1 = "'" + date_1 + "'"
    date_2 = "'" + date_2 + "'"

    total_apps_query = "select s.name, count(ca.id) as all_apps from credit_application ca join staff s on ca.staff_id = s.id where cnt_closed_loans = 0 and creation_date between " + date_1 + " and " + date_2 + " and s.name != 'Robot' group by s.name"
    loan_apps_query = "select s.name, count(ca.id) as all_apps from credit_application ca join staff s on ca.staff_id = s.id where cnt_closed_loans = 0 and creation_date between " + date_1 + " and " + date_2 + " and s.name != 'Robot' and loan_id is not null group by s.name"

    total_apps = pd.read_sql_query(total_apps_query, conn)
    loan_apps = pd.read_sql_query(loan_apps_query, conn)


