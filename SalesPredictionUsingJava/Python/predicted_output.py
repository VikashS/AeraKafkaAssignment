
import pandas as pd 

dataset  = pd.read_csv('test.csv', parse_dates=['date'])

dataset['year'] = dataset['date'].apply(lambda a: a.year)
dataset['month'] = dataset['date'].apply(lambda a: a.month)
dataset['day'] = dataset['date'].apply(lambda a: a.day)

plants = pd.get_dummies(dataset['plant_id'])

dataset['plant1'] = plants['plant1']
dataset['plant2'] = plants['plant2']
dataset['plant3'] = plants['plant3']


import pickle

model = pickle.load(open('model.pck','rb'))

['year','month','day','plant1','plant2','plant3']

m = [[2014,1,15,1,0,0]]

dataset['predicted'] = model.predict(dataset[['year','month','day','plant1','plant2','plant3']].values)

dataset.to_csv('predicted.csv')
