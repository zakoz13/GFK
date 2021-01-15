import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint

engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.8:3306/gfk", pool_pre_ping=True)
conn = engine.connect()
dataframe = pd.read_sql_query("select s.name, count(ca.id) as total_income_insurances,(select (count(ca2.id)) as cnt_resale_insurance from credit_application ca2 join anketa_step anst2 on ca2.id = anst2.app_id join staff_action sa2 on ca2.id = sa2.app_id left join staff s2 on ca2.staff_id = s2.id where ca2.creation_date between '2020-11-01' and '2020-12-01' and anst2.step = 'waitingReviewInsurance' and sa2.action = 'resaleInsurance' and ca2.cnt_closed_loans = 0 and ca2.loan_id is not null AND s2.name = s.name group by s2.name) as resale from credit_application ca join anketa_step anst on ca.id = anst.app_id join staff_action sa on ca.id = sa.app_id left join staff s on ca.staff_id = s.id where ca.creation_date between '2020-11-01' and '2020-12-01' and anst.step = 'waitingReviewInsurance' and sa.action in ('resaleInsurance', 'refusalInsurance', 'nextWithoutInsurance') and ca.cnt_closed_loans = 0 group by s.name order by total_income_insurances desc", conn)
df = pd.DataFrame(dataframe)
df["resale %"] = round((df.resale/df.total_income_insurances)*100, 2)
pprint(df)
filter_ = df["resale %"] <= 50
pprint(df[filter_])
pprint(df.describe())
