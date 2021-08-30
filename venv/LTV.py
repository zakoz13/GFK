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
    dict_use = int(input("Преобразовать данные 1 - да, 0 - нет "))

    # noinspection SqlAggregates,PyArgumentList
    ltv_df = pd.DataFrame(pd.read_sql_query("select ca.client_id, ca.creation_date, (select sum from loan_history lh where active_end is null and lh.loan_id = max(ca.loan_id)) as debt, sum((select sum(sum) from incoming_transfer it where l.id = it.loan_id and it.destination != 'body' and ca.cnt_closed_loans = 0 and it.type not in ('write-off-reserves', 'cession', 'refund') and (it.creation_date) between ca.creation_date and (DATE_ADD(ca.creation_date, INTERVAL 90 DAY)))) as sum_90, sum((select sum(sum) from incoming_transfer it where l.id = it.loan_id and it.destination != 'body' and ca.cnt_closed_loans = 0 and it.type not in ('write-off-reserves', 'cession', 'refund') and (it.creation_date) between ca.creation_date and (DATE_ADD(ca.creation_date, INTERVAL 120 DAY)))) as sum_180, sum((select sum(sum) from incoming_transfer it where l.id = it.loan_id and it.destination != 'body' and ca.cnt_closed_loans = 0 and it.type not in ('write-off-reserves', 'cession', 'refund') and (it.creation_date) between ca.creation_date and (DATE_ADD(ca.creation_date, INTERVAL 270 DAY)))) as sum_270, sum((select sum(sum) from incoming_transfer it where l.id = it.loan_id and it.destination != 'body' and ca.cnt_closed_loans = 0 and it.type not in ('write-off-reserves', 'cession', 'refund') and (it.creation_date) between ca.creation_date and (DATE_ADD(ca.creation_date, INTERVAL 360 DAY)))) as sum_360, (select total_overdue from loan l where l.id = max(ca.loan_id)) as overdue, info.* from credit_application ca join loan l on ca.client_id = l.client_id join (select c.id as id, last_name, first_name, middle_name, sex, (timestampdiff(year, birth_date, curdate())) as age, marital_status, child_count, living_terms, w.salary, w.employment_sphere, w.position, w.type_of_organization, adh.city as home_city, adp.city as pasport_city, (select max(cnt_closed_loans) from credit_application ca where ca.client_id = c.id) as cnt_closed_loans from client c join work w on c.id = w.client_id left join address adh on c.home_address_id = adh.id left join address adp on c.passport_address_id = adp.id ) as info on info.id = ca.client_id where ca.client_id in (select ca.client_id from credit_application ca where ca.loan_id is not null and ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.cnt_closed_loans = 0) and ca.creation_date between " + add_quotes(date_1) + " and " + add_quotes(date_2) + " and ca.loan_id is not null and ca.cnt_closed_loans = 0 group by ca.client_id order by ca.creation_date;", conn))
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

    ltv_df["population"] = 0
    ltv_df.loc[ltv_df['home_city'] == "Санкт-Петербург", "population"] = "2"
    ltv_df.loc[ltv_df['home_city'] == "Москва", "population"] = "2"
    ltv_df.loc[ltv_df['home_city'] == "Новосибирск", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Екатеринбург", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Казань", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Нижний Новгород", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Челябинск", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Самара", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Омск", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Ростов-на-Дону", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Уфа", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Красноярск", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Воронеж", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Пермь", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Волгоград", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Краснодар", "population"] = "1"
    ltv_df.loc[ltv_df['home_city'] == "Саратов", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Тюмень", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Тольятти", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Ижевск", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Барнаул", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Ульяновск", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Иркутск", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Хабаровск", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Ярославль", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Владивосток", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Махачкала", "population"] = "0.5-1"
    ltv_df.loc[ltv_df['home_city'] == "Томск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Оренбург", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Кемерово", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Новокузнецк", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Рязань", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Наб.Челны", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Астрахань", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Пенза", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Киров", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Липецк", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Балашиха", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Чебоксары", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Калининград", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Тула", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Курск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Ставрополь", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Севастополь", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Сочи", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Улан-Удэ", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Тверь", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Магнитогорск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Иваново", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Брянск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Белгород", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Сургут", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Владимир", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Чита", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Нижний Тагил", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Архангельск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Симферополь", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Калуга", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Смоленск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Волжский", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Якутск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Саранск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Череповец", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Курган", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Вологда", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Орёл", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Подольск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Грозный", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Владикавказ", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Тамбов", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Мурманск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Петрозаводск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Нижневартовск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Кострома", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Стерлитамак", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Новороссийск", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Йошкар-Ола", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Химки", "population"] = "0.25-0.5"
    ltv_df.loc[ltv_df['home_city'] == "Таганрог", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Комсомольск-на-Амуре", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Сыктывкар", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Нижнекамск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Нальчик", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Мытищи", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Шахты", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Дзержинск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Энгельс", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Орск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Благовещенск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Братск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Королёв", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Великий Новгород", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Ангарск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Старый Оскол", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Псков", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Люберцы", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Южно-Сахалинск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Бийск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Прокопьевск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Армавир", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Балаково", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Абакан", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Рыбинск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Северодвинск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Норильск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Петропавловск-Камчатский", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Красногорск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Уссурийск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Волгодонск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Новочеркасск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Сызрань", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Каменск-Уральский", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Златоуст", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Альметьевск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Электросталь", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Керчь", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Миасс", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Салават", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Пятигорск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Копейск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Находка", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Хасавюрт", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Рубцовск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Майкоп", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Коломна", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Березники", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Домодедово", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Ковров", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Одинцово", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Нефтекамск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Кисловодск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Батайск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Нефтеюганск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Новочебоксарск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Серпухов", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Щёлково", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Дербент", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Каспийск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Черкесск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Новомосковск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Назрань", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Раменское", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Первоуральск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Кызыл", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Орехово-Зуево", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Новый Уренгой", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Обнинск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Невинномысск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Долгопрудный", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Октябрьский", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Димитровград", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Ессентуки", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Камышин", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Евпатория", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Реутов", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Пушкино", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Жуковский", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Муром", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Ноябрьск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Новошахтинск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Северск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Артем", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Ачинск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Бердск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Ногинск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Арзамас", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Элиста", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Елец", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Ханты-Мансийск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Новокуйбышевск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Железногорск", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Сергиев Посад", "population"] = "0.1-0.25"
    ltv_df.loc[ltv_df['home_city'] == "Зеленодольск", "population"] = "0.1-0.25"

    if dict_use == 1:
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
            2: "Microbusiness (<=15 workers)",
            3: "Smallbusiness (<=100 workers)",
            4: "Mediumbusiness (<=250 workers)",
            5: "Bigbusiness (<3000 workers)",
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

    cols = ltv_df.columns.tolist()
    cols.remove("id")
    cols.insert(7, "PNL")
    cols.insert(8, "category")
    cols.insert(23, "population")
    cols.pop()
    cols.pop()
    cols.pop()

    ltv_df = ltv_df[cols]

    # print(ltv_df)
    ltv_df.to_excel("LTV " + add_quotes(date_1) + " - " + add_quotes(str(datetime.date(pd.to_datetime(date_2) - pd.offsets.Day(1)))) + ".xlsx", sheet_name="ltv")
    # ltv_df.to_csv("loan date " + add_quotes(date_1) + " - " + add_quotes(str(datetime.date(pd.to_datetime(date_2) - pd.offsets.Day(1)))) + ".csv")


ltv()
