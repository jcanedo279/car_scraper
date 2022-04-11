import pandas as pd
import json
import os

from util_scraper import *


#### CAR DATA ####


trim_indices = (3,-2)


#### DATA FEATURES ####
cols = ['car_title', 'car_year', 'price','mileage','transmission','ext_color','int_color','drivetrain','title','accident','owners']
cat_cols = ['body_type', 'transmission', 'ext_color', 'int_color', 'drivetrain', 'trim', 'title', 'owners']


file_path = os.path.abspath(__file__)
cats_path = f'data/{sns.car_name}_cats_to_labels.json'
cleaned_path = f'data/{sns.car_name}_clean.csv'





####### JOIN DATA #######

def concat_path(_file):
    return f'data_{sns.car_name}/{_file}'
files =  list( map(concat_path, os.listdir('/'.join(file_path.split('/')[:-1])+f'/data_{sns.car_name}')) )


df = pd.concat(
    map(pd.read_csv, files)
)


def map_body_type(car_title):
    body_type = car_title.split(' ')[-2]
    if body_type.lower() not in {"coupe", "convertible", "cabriolet"}:
        return "other"
    return body_type.lower()

# [trim_indices[0]:trim_indices[1]]
df['trim'] = df['car_title'].apply(lambda x: ' '.join( x.split(' ')[trim_indices[0]:trim_indices[1]] ))
df['body_type'] = df['car_title'].apply(lambda x: map_body_type(x) )
df['car_year'] = df['car_title'].apply(lambda x: int(x.split(' ')[0]) )

df['car_year'] = p_911_year_to_gen(df['car_year'].to_numpy())

df['accident'] = df['accident'].apply(lambda x: '1' if x=='one' else x)

# df = df[df.groupby('trim').trim.transform('count') >= 50]
# df = df[df.groupby('owners').trim.transform('count') >= 50]
# df = df[df.groupby('car_year').trim.transform('count') >= 30]

df = df[df['price'] <= 150000]
# print(df.head())
df['price'] = df['price'].apply(lambda x: int(x))
# print(df.head())

####### CONVERT TO CATEGORICAL #######

df_cat_cols = df[cat_cols].apply(lambda x: pd.factorize(x)[0])


cats_to_labels = {}
for (feature,ser),(_,ser2) in zip(df[cat_cols].iteritems(), df_cat_cols.iteritems()):
    cat_to_label = {}
    for (_,val),(_,val2) in zip(ser.iteritems(), ser2.iteritems()):
        cat_to_label[val2] = val
    cats_to_labels[feature] = cat_to_label

with open(cats_path, 'w') as fw:
    json.dump(cats_to_labels, fw)

df[cat_cols] = df_cat_cols


print(f'Length of DataFrame is: {df.shape[0]}, with {df.shape[1]} features')

df.to_csv(cleaned_path, index=False, encoding="UTF-8")


