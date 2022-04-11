from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

import pandas as pd
from numba import njit
import numpy as np

import os
import time

from torch.utils.data import Dataset

from types import SimpleNamespace




###########################
## CONFIGURE ENVIRONMENT ##
###########################

PATH_SETTINGS = "settings.cfg"
def parse_settings():
    settings = {}
    with open(PATH_SETTINGS, 'r') as file_settings:
        for line in file_settings.readlines():
            if "#" in line or line is "\n": continue
            if line.endswith('\n'): line = line[:-1]
            line = line.split('=')
            
            name = line[0].strip()
            
            value = line[1].strip(" \"")
            if value.isnumeric(): value = int(value)
            if name == 'search_mode':
                value = [val.strip("()' \"") for val in value.split(',')]
            
            settings[name] = value
    
    return settings


settings = parse_settings()
sns = SimpleNamespace(**settings)





##########
## DATA ##
##########

# clean_data = "data/_{sns.car_name}_clean.csv"
cleaned_path = f'data/{sns.car_name}_clean.csv'
nonoutlier_path = f'data/{sns.car_name}_no.csv'

cats_path = f'data/{sns.car_name}_cats_to_labels.json'
norm_path = f'data/{sns.car_name}_cats_to_norm.json'



car_dir = f'data/data_{sns.car_name}/'
if not os.path.exists(car_dir):
    os.mkdir(car_dir)

CNST_SLEEP = 0.125
def skip_datapoint(driver):
    driver.back()
    time.sleep(CNST_SLEEP)


def raw_data_i(i):
    return f"data/data_{sns.car_name}/{sns.car_name}_raw_{i}.csv"


def push_data(data, page):
    df_i = pd.DataFrame(data)
    df_i.to_csv(raw_data_i(page), columns=df_i.columns, index=False, encoding="ascii")





##################
## WEB SCRAPING ##
##################

class DriverHandler():

    def __init__(self, link):
        self.link = link
        
        self.chrome_options = Options()
        
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--disable-notifications")
        self.chrome_options.add_argument("--log-level=0")
        self.chrome_options.add_argument('--no-sandbox') 
        
        self.chromedriver = ChromeDriverManager().install()
        
        os.environ["webdriver.chrome.driver"] = self.chromedriver

    def __enter__(self):
        self.driver = webdriver.Chrome(self.chromedriver, options=self.chrome_options)
        self.driver.get(self.link)
        
        return self.driver

    def __exit__(self, exc_type,exc_value, exc_traceback):
        print("\nContext scraper closed!")
        self.driver.quit()





########################
## DATA PREPROCESSING ##
########################

def remove_df_outliers(df):
    Q1 = df.quantile(0.1)
    Q3 = df.quantile(0.9)
    IQR = Q3 - Q1
    trueList = ~((df < (Q1 - 1.5 * IQR)) |(df > (Q3 + 1.5 * IQR)))
    return trueList

# @njit
def p_911_year_to_gen(arr):
    bins = np.empty(arr.shape[0])
    
    year_to_bin = [
    ((1963,1972),0), ((1973,1989),1), ((1990,1994),2), ((1996,1998),4), ## 1-st gen, g-series, 964, 993
    ((2000,2003),6), ((2005,2008),8), ((2009,2010),9), ((2014,2014),13), ##  996, 997.1, 997.2, 991.1
    ((2017,2018),16), ((2020,2023),18), ## 991.2, 992.1
    ((1995,1995),3), ((1999,1999),5), ((2004,2004),7),
    ((2011,2011),10), ((2012,2012),11), ((2013,2013),12),
    ((2015,2015),14), ((2016,2016),15),
    ((2019,2019),17)
    ]
    
    for idx, x in enumerate(arr):
        for (y_s,y_e),_bin in year_to_bin:
            if y_s <= x <= y_e:
                bins[idx] = _bin
                continue
    return bins

def map_body_type(car_title):
    body_type = car_title.split(' ')[-2]
    if body_type.lower() not in {"coupe", "convertible", "cabriolet"}:
        return "other"
    return body_type.lower()

def map_traction(car_title):
    traction = car_title.split(' ')[-1]
    return traction

def map_transmission(transmission):
    if 'manual' in transmission.lower(): return 'manual'
    return 'automatic'


# import torch
# import math
# class Car_Dataset(Dataset):
#     def __init__(self, root, pred):
#         self.df = pd.read_csv(root)
#         self.data = self.df.to_numpy()
        
#         indices = [self.df.columns.to_list().index(pred)]
#         print(indices)
#         X = np.delete(self.data, indices, axis=0)
        
#         self.x , self.y = (torch.from_numpy(X),
#                            torch.from_numpy(self.data[:,indices[0]]))
#     def __getitem__(self, idx):
#         return self.x[idx, :], self.y[idx,:]
#     def __len__(self):
#         return len(self.data)


