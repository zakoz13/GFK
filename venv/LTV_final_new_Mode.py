import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import DateOffset

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.170:3306/gfk", pool_pre_ping=True)
conn = engine.connect()


def ltv():

    global result_debt, c, d, interval, client_search

    def add_quotes(q):
        return "'" + q + "'"

    mode = (input("Поиск по клиентам или по дате c/d "))
    interval = int(input("Введите количество дней мониторинга "))

    if mode == 'c':
        clients_df = pd.DataFrame(pd.read_sql_query("select distinctrow ca.client_id from credit_application ca where ca.client_id in ();", conn))
        mode = clients_df
    elif mode == 'd':
        date_1 = input("Введите дату начала мониторинга в формате YYYY-MM-DD: ")
        date_2 = input("Введите дату окончания мониторинга  в формате YYYY-MM-DD: ")
        date_df = pd.DataFrame(pd.read_sql_query("select distinctrow ca.client_id from credit_application ca where date(creation_date) between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.loan_id is not null and ca.cnt_closed_loans = 0;", conn))
        mode = date_df

    search_clients = []
    for client_id in mode.client_id:
        try:
            client_search = pd.DataFrame(pd.read_sql_query("select ca.client_id, date(ca.creation_date) as date, ifnull(((select distinctrow (select lrs.remaining_body from loan_retro_stat lrs where lrs.loan_id = ca3.loan_id and date(lrs.date) = (DATE_ADD(date(ca3.creation_date), INTERVAL " + str(interval) + " DAY ))) from credit_application ca3 where date(ca3.creation_date) = (select distinctrow date(ca.creation_date) from credit_application ca where ca3.client_id = ca.client_id and ca.cnt_closed_loans = 0 and ca.loan_id is not null) and ca3.client_id = ca.client_id group by ca3.client_id)), 0) as debt_" + str(interval) + ", ifnull(sum((select sum(sum) from incoming_transfer it where l.id = it.loan_id and it.destination != 'body' and ca.cnt_closed_loans = 0 and it.type not in ('write-off-reserves', 'cession', 'refund') and (it.creation_date) between date(ca.creation_date) and (DATE_ADD(date(ca.creation_date), INTERVAL " + str(interval) + " DAY)))), 0) as sum_" + str(interval) + " from credit_application ca join loan l on ca.client_id = l.client_id where ca.client_id = " + str(client_id) + " and ca.loan_id is not null and ca.cnt_closed_loans = 0 group by ca.client_id order by date(ca.creation_date);", conn))
            search_clients.append(client_search)
        except sal.exc.DBAPIError:
            client_search.debt = 0

        result_debt = pd.concat(search_clients, ignore_index=True)
        cols_deb = result_debt.columns.tolist()
        result_debt = result_debt[cols_deb]

    final = result_debt
    final["PNL"] = final["sum_" + str(interval) + ""] - final["debt_" + str(interval) + ""]

    final["category"] = 0
    final.loc[final["PNL"] >= 30000, "category"] = "30+"
    final.loc[(final["PNL"] >= 20000) & (final["PNL"] <= 29999), "category"] = "20-29.9"
    final.loc[(final["PNL"] >= 10000) & (final["PNL"] <= 19999), "category"] = "10-19.9"
    final.loc[(final["PNL"] >= 6000) & (final["PNL"] <= 9999), "category"] = "6-9.9"
    final.loc[(final["PNL"] >= 3000) & (final["PNL"] <= 5999), "category"] = "3-5.9"
    final.loc[(final["PNL"] >= 0) & (final["PNL"] <= 2999), "category"] = "0-2.9"
    final.loc[final["PNL"] < 0, "category"] = "<0"

    # print(final)
    final.to_excel("LTV" + str(interval) + ".xlsx", sheet_name="ltv")
    # final.to_csv("LTV + ".csv")


ltv()
