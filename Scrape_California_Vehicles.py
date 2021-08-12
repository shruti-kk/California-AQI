#!/usr/bin/env python
# coding: utf-8

# In[1]:


import urllib
import requests
import json
import time
import sqlite3
import pandas as pd
from bs4 import element
from bs4 import BeautifulSoup

# SQLite table for ZIPCODES Vehicles Database
#create table
def vehicles_table_create():
    con = sqlite3.connect('Dataset/Project_DataBase_California_Scraped.db')
    cur = con.cursor()
    # Drop table
    cur.executescript('DROP TABLE if exists California_Zipcodes_Vehicles')
    # Create table
    cur.execute('''CREATE TABLE California_Zipcodes_Vehicles
                (Duty text, Make text, Vehicles real, Zip_Code text,
                 Fuel text, Date text, _id integer PRIMARY KEY, Model_Year integer)''')
    print("Table 'California_Zipcodes_Vehicles' created in DB 'Project_DataBase_California_Scraped_5' in the folder 'Dataset'!")
    print("Table 'California_Zipcodes_Vehicles' is used to store Vehicles data of all Zipcodes of California from API https://data.ca.gov")
    cur.execute("pragma table_info('California_Zipcodes_Vehicles')")    
    columns_data = cur.fetchall()
    col_names = []
    for val in columns_data:
            col_names.append(val[1])
    print("\nColumns in 'California_Zipcodes_Vehicles' table: ",col_names)
    con.close()

#create table to store data for --scrape option
def vehicles_table_create_5():
    con = sqlite3.connect('Dataset/Project_DataBase_California_Scraped_5.db')
    cur = con.cursor()
    # Drop table
    cur.executescript('DROP TABLE if exists California_Zipcodes_Vehicles')
    # Create table
    cur.execute('''CREATE TABLE California_Zipcodes_Vehicles
                (Duty text, Make text, Vehicles real, Zip_Code text,
                 Fuel text, Date text, _id integer PRIMARY KEY, Model_Year integer)''')
    print("Table 'California_Zipcodes_Vehicles' created in DB 'Project_DataBase_California_Scraped_5' in the folder 'Dataset'!")
    print("Table 'California_Zipcodes_Vehicles' is used to store Vehicles data of all Zipcodes of California from API https://data.ca.gov")
    cur.execute("pragma table_info('California_Zipcodes_Vehicles')")    
    columns_data = cur.fetchall()
    col_names = []
    for val in columns_data:
            col_names.append(val[1])
    print("\nColumns in 'California_Zipcodes_Vehicles' table: ",col_names)
          
    con.close()

#Insert 1 row    
def vehicles_table_insert_row(data,db_name):
    con = sqlite3.connect('Project_DataBase_California.db')
    cur = con.cursor()
    # Insert data
    try:
        cur.execute('INSERT or IGNORE INTO California_Zipcodes_Vehicles VALUES (?,?,?,?,?,?,?,?)', data)    
        # Save (commit) the changes
        con.commit()
    except:
        con.close()
        pass
    con.close()

#Insert multiple rows
def vehicles_table_insert_rows(dataset,db_name):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    print("\nInserting Vehicles data in table 'California_Zipcodes_Vehicles'...")
    # Insert data
    cur.executemany('INSERT or IGNORE INTO California_Zipcodes_Vehicles VALUES (?,?,?,?,?,?,?,?)', dataset)    
    # Save (commit) the changes
    con.commit()        
    con.close()
    print("Vehicles data inserted in table 'California_Zipcodes_Vehicles' ! ")
    
def get_vehicles_table_data(db_name,rows):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    sql_limit = (rows,)
    cur.execute("SELECT * FROM California_Zipcodes_Vehicles LIMIT ?",sql_limit)
    vehicles = cur.fetchall()
    print('\nSample Data ('+str(rows)+' rows) from Table California_Zipcodes_Vehicles:\n') 
    
    for row in vehicles:
        print(row)

    con.close() 

#Function for API call
def get_vehicles_data_from_API(offset,flag):
    vehicles_data =[]
    if flag ==1:
        api = "https://data.ca.gov/api/3/action/datastore_search?resource_id=4254a06d-9937-4083-9441-65597dd267e8&limit=5&offset=" + str(offset) 
    else:
        api = "https://data.ca.gov/api/3/action/datastore_search?resource_id=4254a06d-9937-4083-9441-65597dd267e8&limit=20000&offset=" + str(offset)        
    r = requests.get(api)
    content = r.content
    api_data = json.loads(content)
    #print(r.url)
    for record in api_data['result']['records']:
        vehicles_data.append(tuple(record.values())) 
    
    return vehicles_data

def get_zips_vehicles_data(rows):
    path = 'Dataset/'
    if(isinstance(rows,int)):
        vehicles_table_create_5()
        DB_name = "Project_DataBase_California_Scraped_5.db"
        flag = 1
    else:
        vehicles_table_create()
        DB_name = "Project_DataBase_California_Scraped.db"
        flag = 0
        
    #602394 - Total number of rows
    offset = 0
    print("Fetching geocodes of zipcodes from API..")
    while offset<602394:
        result = get_vehicles_data_from_API(offset,flag) 
        #print(result)
        offset+=20000
        vehicles_table_insert_rows(result,path+DB_name)  
        
        if(rows == 5):
            break



