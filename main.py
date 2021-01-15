import pandas as pd
# import numpy as np

sa_november = pd.read_csv(r'C:\Users\Zak\Desktop\staff_action_november.csv', delimiter=',')
insurance = pd.read_csv(r'C:\Users\Zak\Desktop\insurance_november.csv', delimiter=',')
# print(data.info())
# print(len(data))
# print(data.describe())
# axis = data.append(data.sum(axis=0), ignore_index=True)
# print(axis)
# f = data[data['percent_overdue_micr'] <= 60]
# print(f)
# sort = data.sort_values('percent_overdue_micr', ascending=False)
# print(sort)
print(sa_november)
print(insurance)

