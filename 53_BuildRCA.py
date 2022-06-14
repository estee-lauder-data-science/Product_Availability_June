# @author:          vigovind
# @last_updated:    5/2/2020

import pyodbc
import pandas as pd
#import numpy as np
import time

start_time = time.time() 

output_filename = './Data/RCA_Pub_Result.csv'

#database credentials
SERVER = 'US-THT-NEMESIS1'
DATABASE = 'SC_PLN_DS'
USERNAME = 'SC_DS_LOGIN'
PASSWORD = 'SC_DS_ELC'


index = ['SKU_10D','LOCATION','FISCAL_MONTH','FISCAL_YEAR','MARKET','RUN_TYPE','CYCLE','DEMAND_PRIORITY_12D']
output_columns = ['PRIMARY_RISK_UNITS','PRIMARY_RISK_UNITS_DRIVER','PRIMARY_RISK_DOLLARS','PRIMARY_RISK_DOLLARS_DRIVER']

def connect(SERVER,DATABASE,USERNAME,PASSWORD):
    cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER='+SERVER+';DATABASE='+DATABASE+';UID='+USERNAME+';PWD='+ PASSWORD+';TRUSTED_CONNECTION=NO')
    return cnxn

cnxn = connect(SERVER, DATABASE, USERNAME, PASSWORD)
query_names = {
                 "units":     "./SQL/53_RCA_SQL_UNITS.sql",
                 "dollars":   "./SQL/53_RCA_SQL_DOLLARS.sql"
              }
results = {}

for key, value in query_names.items():
    with open(value, 'r') as file:
        query = file.read()
    raw_data = pd.read_sql_query(query,cnxn)
    raw_data.drop_duplicates(inplace=True)
    raw_data.set_index(index, inplace=True)
    raw_data['PRIMARY_RISK_' + key.upper()] = raw_data.max(axis=1)    
    raw_data['PRIMARY_RISK_' + key.upper() + '_DRIVER'] = raw_data.idxmax(axis=1)
    results[key] = raw_data

units = results['units']
dollars = results['dollars']
final_result = pd.merge(units, dollars, how = 'inner', left_index = True, right_index = True)
final_result = final_result[output_columns]
final_result.reset_index(inplace = True)
final_result = final_result[(final_result['PRIMARY_RISK_UNITS'] > 0)]
final_result = final_result.drop_duplicates()
final_result.to_csv(output_filename, index=False)

write_time = time.time() 
print("\n Total Run Time: ",round((write_time- start_time)/60, 2), "minutes")
x=input('Complete')
