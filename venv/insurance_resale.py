import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint


def report():
    engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.8:3306/gfk", pool_pre_ping=True)
    conn = engine.connect()

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    date_1 = input("Введите дату от в формате YYYY-MM-DD: ")
    date_2 = input("Введите дату до в формате YYYY-MM-DD: ")
    date_1 = "'" + date_1 + "'"
    date_2 = "'" + date_2 + "'"
    loan_gen = int(input("Введите поколение займа: 1 для первичников, 2 для вторичников, 0 для всех вместе - "))

    total_ins_query = "select staff_name ,count(credit_app_id) as total_apps from(select distinctrow ca.id credit_app_id, sa.action, s.id staff_id, s.name staff_name, ca.creation_date, ca.loan_id from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on sa.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and ca.id in ( select (ca.id) as check_app from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on ca.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and anst.step = 'waitingReviewInsurance' and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') and ca.cnt_closed_loans >= 0 ) and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') ) as total_ins group by staff_name"
    total_first_loan_query = "select staff_name ,count(credit_app_id) as total_apps from(select distinctrow ca.id credit_app_id, sa.action, s.id staff_id, s.name staff_name, ca.creation_date, ca.loan_id from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on sa.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and ca.id in ( select (ca.id) as check_app from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on ca.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and anst.step = 'waitingReviewInsurance' and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') and ca.cnt_closed_loans = 0 ) and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') ) as total_ins group by staff_name"
    total_second_gen_loan_query = "select staff_name ,count(credit_app_id) as total_apps from(select distinctrow ca.id credit_app_id, sa.action, s.id staff_id, s.name staff_name, ca.creation_date, ca.loan_id from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on sa.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and ca.id in ( select (ca.id) as check_app from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on ca.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and anst.step = 'waitingReviewInsurance' and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') and ca.cnt_closed_loans > 0 ) and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') ) as total_ins group by staff_name"

    resale_ins_query_all = "select staff_name, count(credit_app_id) as resale_apps from(select distinctrow ca.id credit_app_id, sa.action, s.id staff_id, s.name staff_name, ca.creation_date, ca.loan_id from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on sa.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and ca.id in ( select (ca.id) as check_app from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on ca.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and anst.step = 'waitingReviewInsurance' and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') and ca.cnt_closed_loans >= 0 ) and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') ) as resale where action = 'resaleInsurance' and loan_id is not null group by staff_name"
    resale_ins_query_first_loan = "select staff_name, count(credit_app_id) as resale_apps from(select distinctrow ca.id credit_app_id, sa.action, s.id staff_id, s.name staff_name, ca.creation_date, ca.loan_id from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on sa.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and ca.id in ( select (ca.id) as check_app from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on ca.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and anst.step = 'waitingReviewInsurance' and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') and ca.cnt_closed_loans = 0 ) and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') ) as resale where action = 'resaleInsurance' and loan_id is not null group by staff_name"
    resale_ins_query_second_gen = "select staff_name, count(credit_app_id) as resale_apps from(select distinctrow ca.id credit_app_id, sa.action, s.id staff_id, s.name staff_name, ca.creation_date, ca.loan_id from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on sa.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and ca.id in ( select (ca.id) as check_app from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on ca.staff_id = s.id where ca.creation_date between " + date_1 + " and " + date_2 + " and anst.step = 'waitingReviewInsurance' and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') and ca.cnt_closed_loans > 0 ) and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') ) as resale where action = 'resaleInsurance' and loan_id is not null group by staff_name"

    total_insurance = pd.read_sql_query(total_ins_query, conn)
    total_first_loan = pd.read_sql_query(total_first_loan_query, conn)
    total_second_gen_loan = pd.read_sql_query(total_second_gen_loan_query, conn)

    resale_insurance = pd.read_sql_query(resale_ins_query_all, conn)
    resale_insurance_first_loan = pd.read_sql_query(resale_ins_query_first_loan, conn)
    resale_insurance_second_gen = pd.read_sql_query(resale_ins_query_second_gen, conn)

    if loan_gen == 0:
        x = total_insurance
        y = resale_insurance
    elif loan_gen == 1:
        x = total_first_loan
        y = resale_insurance_first_loan
    elif loan_gen == 2:
        x = total_second_gen_loan
        y = resale_insurance_second_gen
    t_i = pd.DataFrame(x)
    r_i = pd.DataFrame(y)
    rep = t_i.merge(r_i)
    rep["resale %"] = round((rep.resale_apps/rep.total_apps)*100, 0)
    pprint(rep)


report()
