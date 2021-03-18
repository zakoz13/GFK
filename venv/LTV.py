import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint
from datetime import datetime
from pandas.tseries.offsets import DateOffset
import cubes

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.170:3306/gfk", pool_pre_ping=True)
conn = engine.connect()


def ltv():

    def add_quotes(q):
        return "'" + q + "'"

    date_1 = input("Введите дату начала мониторинга в формате YYYY-MM-DD: ")
    date_2 = input("Введите дату окончания мониторинга  в формате YYYY-MM-DD: ")

    # noinspection SqlAggregates,PyArgumentList
    ltv_df = pd.DataFrame(pd.read_sql_query("select ca.client_id, ca.creation_date, (select sum from loan_history lh where active_end is null and lh.loan_id = max(ca.loan_id)) as debt, sum((select sum(sum) from incoming_transfer it where l.id = it.loan_id and it.destination != 'body' and (it.creation_date) between ca.creation_date and (DATE_ADD(ca.creation_date, INTERVAL 90 DAY)))) as sum_90, sum((select sum(sum) from incoming_transfer it where l.id = it.loan_id and it.destination != 'body' and (it.creation_date) between ca.creation_date and (DATE_ADD(ca.creation_date, INTERVAL 180 DAY)))) as sum_180, sum((select sum(sum) from incoming_transfer it where l.id = it.loan_id and it.destination != 'body' and (it.creation_date) between ca.creation_date and (DATE_ADD(ca.creation_date, INTERVAL 270 DAY)))) as sum_270, sum((select sum(sum) from incoming_transfer it where l.id = it.loan_id and it.destination != 'body' and (it.creation_date) between ca.creation_date and (DATE_ADD(ca.creation_date, INTERVAL 360 DAY)))) as sum_360, (select total_overdue from loan l where l.id = max(ca.loan_id)) as overdue, info.* from credit_application ca join loan l on ca.client_id = l.client_id join (select c.id as id, last_name, first_name, middle_name, sex, (timestampdiff(year, birth_date, curdate())) as age, marital_status, child_count, living_terms, w.salary, w.employment_sphere, w.position, w.type_of_organization, adh.city as home_city, adp.city as pasport_city, (select max(cnt_closed_loans) from credit_application ca where ca.client_id = c.id) as cnt_closed_loans from client c join work w on c.id = w.client_id left join address adh on c.home_address_id = adh.id left join address adp on c.passport_address_id = adp.id ) as info on info.id = ca.client_id where ca.loan_id is not null and ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.cnt_closed_loans = 0 group by ca.client_id order by ca.creation_date;", conn))
    ltv_df.fillna(0, inplace=True)

    ltv_df["PNL"] = ltv_df["sum_180"] - ltv_df["debt"]

    ltv_df["category"] = 0
    ltv_df.loc[ltv_df["PNL"] >= 30000, "category"] = "30+"
    ltv_df.loc[(ltv_df["PNL"] >= 20000) & (ltv_df["PNL"] <= 29999), "category"] = "20-29.9"
    ltv_df.loc[(ltv_df["PNL"] >= 10000) & (ltv_df["PNL"] <= 19999), "category"] = "10-19.9"
    ltv_df.loc[(ltv_df["PNL"] >= 6000) & (ltv_df["PNL"] <= 9999), "category"] = "6-9.9"
    ltv_df.loc[(ltv_df["PNL"] >= 3000) & (ltv_df["PNL"] <= 5999), "category"] = "3-5.9"
    ltv_df.loc[(ltv_df["PNL"] >= 0) & (ltv_df["PNL"] <= 2999), "category"] = "0-2.9"
    ltv_df.loc[ltv_df["PNL"] < 0, "category"] = "<0"

    def dict_replace():

        sex = {
            "0": "man",
            "1": "woman"
        }
        ltv_df["sex"].replace(sex, inplace=True)

        marital_status = {
            1: "married",
            2: "single",
            3: "divorced",
            4: "widowed"
        }
        ltv_df["marital_status"].replace(marital_status, inplace=True)

        living_terms = {
            1: "own",
            2: "rental",
            3: "parents",
            4: "other",
            5: "municipal",
        }
        ltv_df["living_terms"].replace(living_terms, inplace=True)

        employment_sphere = {
            1: "Autoservices",
            2: "Bank",
            3: "Beauty",
            4: "Veterinary",
            5: "Military",
            6: "Government",
            7: "Personal services",
            8: "Entertainment",
            9: "Engineering",
            10: "Art",
            12: "IT",
            13: "Cleaning",
            14: "Logistics",
            15: "Marketing",
            16: "Medicine gov",
            17: "Medicine private",
            18: "Science",
            19: "Real estate",
            20: "Education",
            21: "Catering",
            22: "Food industry",
            23: "Industry",
            24: "Agriculture",
            25: "Security forces",
            26: "Sport",
            27: "Building",
            28: "Court",
            29: "Security",
            30: "Commerce",
            31: "Transport",
            32: "Tourism",
            33: "Finance",
            34: "FSSP",
            35: "Legal (notary) services",
            36: "Production",
            37: "Municipal services",
            38: "Telecom",
            39: "Energy",
            40: "Insurance",
            41: "Mining",
            42: "Avia",
            43: "Train",
            44: "Hotel",
            45: "Religion",
            46: "Social care",
            47: "Casino",
            48: "Power supply",
            49: "Media",
            50: "Consulting",
            51: "Vehicle trade",
            52: "Wholesale trade",
            53: "Chemical industry",
            54: "Wood industry",
            55: "Processing industry"
        }
        ltv_df["employment_sphere"].replace(employment_sphere, inplace=True)

        type_of_organization = {
            1: "Selfwork",
            2: "Microbuisnes (<=15 workers)",
            3: "Smallbuisnes (<=100 workers)",
            4: "Mediumbuisnes (<=250 workers)",
            5: "Bigbuisnes (<3000 workers)",
            6: "Corporation (>3000 workers)",
            7: "Another",
            8: "Unemployed",
            9: "Retiree"
        }
        ltv_df["type_of_organization"].replace(type_of_organization, inplace=True)

        position = {
            1: "Unskilled employee",
            2: "Highskilled employee",
            3: "Middle manager",
            4: "Top manager"
        }
        ltv_df["position"].replace(position, inplace=True)

    dict_replace()

    cols = ltv_df.columns.tolist()
    cols.remove("id")
    cols.insert(7, 'PNL')
    cols.insert(8, 'category')
    cols.pop()
    cols.pop()

    ltv_df = ltv_df[cols]

    print(ltv_df)
    # ltv_df.to_excel("loan date " + add_quotes(date_1) + " - " + add_quotes(str(datetime.date(pd.to_datetime(date_2) - pd.offsets.Day(1)))) + ".xlsx", sheet_name="ltv")
    # # ltv_df.to_csv("loan date " + add_quotes(date_1) + " - " + add_quotes(str(datetime.date(pd.to_datetime(date_2) - pd.offsets.Day(1)))) + ".csv")


ltv()
