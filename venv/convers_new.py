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
    n = int(input("Введите период конверсии(дни):  "))

    clients = pd.DataFrame(pd.read_sql_query("select l.client_id, l.num as cnt_all from loan l where l.close_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and l.loan_status_id = 4;", conn))

    search_clients = []
    for client_id in clients.client_id:

        # client_search = pd.DataFrame(pd.read_sql_query("select ca.client_id, ca.cnt_closed_loans as cnt_converse, min(l3.close_date) as min_close_date, ca.creation_date from credit_application ca join loan l3 on ca.loan_id = l3.id where ca.creation_date between (select min(l.close_date) from loan l where l.close_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and l.loan_status_id = 4 and l.client_id = " + str(client_id) + " group by l.client_id) and (select date_add(MIN(l2.close_date),INTERVAL " + str(n) + " DAY) AS interval_days from loan l2 where l2.close_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and l2.loan_status_id = 4 and l2.client_id = " + str(client_id) + " group by l2.client_id) and l3.client_id = " + str(client_id) + " and ca.loan_id is not null and l3.close_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + ";", conn))
        client_search = pd.DataFrame(pd.read_sql_query("select ca.client_id, max(ca.cnt_closed_loans) as cnt_converse, min(l3.close_date) as min_close_date, ca.creation_date from credit_application ca join loan l3 on ca.loan_id = l3.id where ca.creation_date between (select min(l.close_date) from loan l where l.close_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and l.loan_status_id = 4 and l.client_id = l3.client_id group by l.client_id) and (select date_add(MIN(l2.close_date),INTERVAL " + str(n) + " DAY) AS interval_days from loan l2 where l2.close_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and l2.loan_status_id = 4 and l2.client_id = l3.client_id group by l2.client_id) and l3.client_id = " + str(client_id) + " and ca.loan_id is not null and l3.close_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + ";", conn))

        search_clients.append(client_search)

    result = pd.concat(search_clients)
    cols = result.columns.tolist()
    result.dropna(inplace=True)
    cols.pop()
    cols.pop()
    result = result[cols]
    result_df = pd.DataFrame(result)
    result_df["loan_gen"] = 0
    result_df.loc[result_df["cnt_converse"] == 1, "loan_gen"] = "1"
    result_df.loc[result_df["cnt_converse"] == 2, "loan_gen"] = "2"
    result_df.loc[result_df["cnt_converse"] == 3, "loan_gen"] = "3"
    result_df.loc[result_df["cnt_converse"] == 4, "loan_gen"] = "4"
    result_df.loc[result_df["cnt_converse"] == 5, "loan_gen"] = "5"
    result_df.loc[result_df["cnt_converse"] == 6, "loan_gen"] = "6"
    result_df.loc[result_df["cnt_converse"] == 7, "loan_gen"] = "7"
    result_df.loc[result_df["cnt_converse"] >= 8, "loan_gen"] = "8+"
    res = result_df.groupby(['loan_gen']).count()
    res.pop("client_id")

    clients_df = pd.DataFrame(clients)
    clients_df["loan_gen"] = 0
    clients_df.loc[clients_df["cnt_all"] == 1, "loan_gen"] = "1"
    clients_df.loc[clients_df["cnt_all"] == 2, "loan_gen"] = "2"
    clients_df.loc[clients_df["cnt_all"] == 3, "loan_gen"] = "3"
    clients_df.loc[clients_df["cnt_all"] == 4, "loan_gen"] = "4"
    clients_df.loc[clients_df["cnt_all"] == 5, "loan_gen"] = "5"
    clients_df.loc[clients_df["cnt_all"] == 6, "loan_gen"] = "6"
    clients_df.loc[clients_df["cnt_all"] == 7, "loan_gen"] = "7"
    clients_df.loc[clients_df["cnt_all"] >= 8, "loan_gen"] = "8+"
    cli = clients_df.groupby(['loan_gen']).count()
    cli.pop("client_id")

    final = pd.merge(cli, res, on="loan_gen")
    print(final)
    # writer = pd.ExcelWriter("converse" + add_quotes(date_1) + " - " + add_quotes(str(datetime.date(pd.to_datetime(date_2) - pd.offsets.Day(1)))) + " days " + str(n) + " .xlsx")
    # final.to_excel(writer, "report")
    # writer.save()


conversion()
