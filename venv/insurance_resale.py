import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint
from datetime import datetime

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def report():
    engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.170:3306/gfk", pool_pre_ping=True)
    conn = engine.connect()

    def add_quotes(q):
        return "'" + q + "'"

    date_1 = input("Введите дату от в формате YYYY-MM-DD: ")
    date_2 = input("Введите дату до в формате YYYY-MM-DD: ")

    while True:
        loan_gen = int(input("Введите поколение займа: 1 для первичников, 2 для вторичников, 0 для всех вместе - "))
        if loan_gen in (0, 1, 2):
            break

    operordict = {0: "ca.cnt_closed_loans >= 0", 1: "ca.cnt_closed_loans = 0", 2: "ca.cnt_closed_loans > 0"}

    total_ins_query = "select staff_name ,count(credit_app_id) as total_apps from(select distinctrow ca.id credit_app_id, sa.action, s.id staff_id, s.name staff_name, ca.creation_date, ca.loan_id from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on sa.staff_id = s.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.id in ( select (ca.id) as check_app from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on ca.staff_id = s.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and anst.step = 'waitingReviewInsurance' and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') and " + operordict[loan_gen] + " ) and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') ) as total_ins group by staff_name"
    resale_ins_query = "select staff_name, count(credit_app_id) as resale_apps from(select distinctrow ca.id credit_app_id, sa.action, s.id staff_id, s.name staff_name, ca.creation_date, ca.loan_id from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on sa.staff_id = s.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.id in ( select (ca.id) as check_app from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on ca.staff_id = s.id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and anst.step = 'waitingReviewInsurance' and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') and " + operordict[loan_gen] + " ) and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') ) as resale where action = 'resaleInsurance' and loan_id is not null group by staff_name"

    total_insurance = pd.read_sql_query(total_ins_query, conn)
    resale_insurance = pd.read_sql_query(resale_ins_query, conn)

    x = total_insurance
    y = resale_insurance

    t_i = pd.DataFrame(x)
    r_i = pd.DataFrame(y)
    rep = t_i.merge(r_i)
    rep["resale %"] = round((rep.resale_apps/rep.total_apps)*100, 0)

    # pprint(rep)
    rep.to_excel("ins_resale " + add_quotes(date_1) + " - " + add_quotes(str(datetime.date(pd.to_datetime(date_2) - pd.offsets.Day(1)))) + " loan_gen  =" + str(loan_gen) + ".xlsx", sheet_name="insurance resale")

    def global_review_apps():
        total_insurance_query = "select count(distinct (ca.id)) as 'total_insurance' from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and anst.step ='waitingReviewInsurance' and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') and " + operordict[loan_gen] + ""
        resale_and_loan_query = "select count(distinct (ca.id)) as 'resale_and_loan' from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and anst.step ='waitingReviewInsurance' and sa.action = 'resaleInsurance' and " + operordict[loan_gen] + " and ca.loan_id is not null"
        resale_and_loan_is_null = "select count(distinct (ca.id)) as 'resale_and_loan_is_null' from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and anst.step ='waitingReviewInsurance' and sa.action = 'resaleInsurance' and " + operordict[loan_gen] + " and ca.loan_id is null"
        next_without_and_loan_query = "select count(distinct (ca.id)) as 'next_without_and_loan' from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and anst.step ='waitingReviewInsurance' and sa.action = 'nextWithoutInsurance' and " + operordict[loan_gen] + " and ca.loan_id is not null"
        next_without_and_loan_is_null_query = "select count(distinct (ca.id)) as 'next_without_and_loan_is_null' from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and anst.step ='waitingReviewInsurance' and sa.action = 'nextWithoutInsurance' and " + operordict[loan_gen] + " and ca.loan_id is null"
        refuse_and_loan_query = "select count(distinct (ca.id)) as 'refuse_and_loan' from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and anst.step ='waitingReviewInsurance' and sa.action = 'refusalInsurance' and " + operordict[loan_gen] + " and ca.loan_id is not null"
        refuse_and_loan_is_null_query = "select count(distinct (ca.id)) as 'refuse_and_loan_is_null' from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id where ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and anst.step ='waitingReviewInsurance' and sa.action = 'refusalInsurance' and " + operordict[loan_gen] + " and ca.loan_id is null"

        total_insurance_all = pd.read_sql_query(total_insurance_query, conn)
        resale_and_loan = pd.read_sql_query(resale_and_loan_query, conn)
        resale_and_loan_is_null = pd.read_sql_query(resale_and_loan_is_null, conn)
        next_without_and_loan = pd.read_sql_query(next_without_and_loan_query, conn)
        next_without_and_loan_is_null = pd.read_sql_query(next_without_and_loan_is_null_query, conn)
        refuse_and_loan = pd.read_sql_query(refuse_and_loan_query, conn)
        refuse_and_loan_is_null = pd.read_sql_query(refuse_and_loan_is_null_query, conn)

        a = pd.DataFrame(total_insurance_all)
        b = pd.DataFrame(resale_and_loan)
        c = pd.DataFrame(resale_and_loan_is_null)
        d = pd.DataFrame(next_without_and_loan)
        e = pd.DataFrame(next_without_and_loan_is_null)
        f = pd.DataFrame(refuse_and_loan)
        g = pd.DataFrame(refuse_and_loan_is_null)

        concat_df = pd.concat((a, b, c, d, e, f, g), ignore_index=True)
        sub_rep = concat_df.dropna(thresh=1)

        # pprint(sub_rep)
        sub_rep.to_excel("total_review " + add_quotes(date_1) + " - " + add_quotes(str(datetime.date(pd.to_datetime(date_2) - pd.offsets.Day(1)))) + " loan_gen  =" + str(loan_gen) + ".xlsx", sheet_name="insurance resale")

    global_review_apps()


report()
