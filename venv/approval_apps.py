import pymysql
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

engine = create_engine("mysql+pymysql://ml_user:U5VVYcxx@46.182.24.8:3306/gfk", pool_pre_ping=True)
conn = engine.connect()
