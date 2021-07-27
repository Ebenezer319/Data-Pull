# -*- coding: utf-8 -*-
"""
Created on Mon Jul 26 21:33:09 2021

@author: ebenezer.an
"""

import requests
import pandas as pd
import string
import numpy as np


# In[Class for Pulling Data]

class DataPull:
    
    def __init__(self, lowercase_letters, path):
        self.lowercase_letters = lowercase_letters
        self.path = path

        
    def __enter__(self):
        return self
    

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self
    
    def pull_files(self):
        dataFrame = pd.DataFrame()

        for letter in self.lowercase_letters:
            data = pd.read_csv(self.path + letter + '.csv')
            dataFrame = pd.concat([dataFrame, data], sort = False, ignore_index=True)
            
            print('Pulling file: ' + letter + '.csv')
    
        return dataFrame
    
    def transform_data(self, d_frame):
        data_pivot = pd.pivot_table(d_frame, values=['length'], index=['user_id'], columns=['path'], aggfunc=np.sum, fill_value=0)
        data_pivot.reset_index(level=data_pivot.index.names, inplace=True)
        
        data_pivot.columns = data_pivot.columns.map(lambda x: (x[1] + " " + x[0].replace('length', '')).strip() if x[1] != '' else (x[0].replace('length', '')).strip())

        return data_pivot
    
    def create_csv(self, pivot, file_name):
        pivot.to_csv(file_name, index = False)
                
        
# In[Change Path to have data pulled as ]

lowercase_alphabets = list(string.ascii_lowercase)
path = 'https://public.wiwdata.com/engineering-challenge/data/'

try:
    
    with DataPull(lowercase_alphabets, path) as data_pull:
        user_data = data_pull.pull_files()
        user_pivot = data_pull.transform_data(user_data)
        data_pull.create_csv(user_pivot, 'User Journey.csv')
        
        
except:
    print("Failed to run, check path")

        