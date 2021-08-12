#!/usr/bin/env python
# coding: utf-8

# In[6]:


import urllib
import requests
import json
import time
import sqlite3
import pandas as pd
import xml.etree.ElementTree as ET
from bs4 import element
from bs4 import BeautifulSoup

# SQLite table for AQI data
#create table
def AQI_table_create():
    con = sqlite3.connect('Dataset/Project_DataBase_California_Scraped.db')
    con.execute("PRAGMA foreign_keys = 1")
    cur = con.cursor()
    # Drop table
    cur.executescript('DROP TABLE if exists California_Zipcodes_AQI')
    # Create table
    cur.execute('''CREATE TABLE California_Zipcodes_AQI
                (Zipcode text,Air_Quality_Index real,Air_Quality_Index_Category text,Dominant_Pollutant text,
                 Carbon_Monoxide real,Nitrogen_Dioxide real, Ozone real,Inhalable_Particulate_Matter_pm10 real,
                 Fine_Particulate_Matter_pm25 real,Sulfur_Dioxide real,
                 FOREIGN KEY(Zipcode) REFERENCES California_Zipcodes_Geocodes(Zipcode))''')
    
    print("Table 'California_Zipcodes_AQI' created in DB 'Project_DataBase_California_Scraped' in the folder 'Dataset'!")
    print("Table 'California_Zipcodes_AQI' is used to store AQI data of all Zipcodes of California from API https://api.breezometer.com/air-quality")
    cur.execute("pragma table_info('California_Zipcodes_AQI')")    
    columns_data = cur.fetchall()
    col_names = []
    for val in columns_data:
            col_names.append(val[1])
    print("\nColumns in 'California_Zipcodes_AQI' table: ",col_names) 
    con.close()
    
#create table to store data for --scrape option
def AQI_table_create_5():
    con = sqlite3.connect('Dataset/Project_DataBase_California_Scraped_5.db')
    cur = con.cursor()
    # Drop table
    cur.executescript('DROP TABLE if exists California_Zipcodes_AQI')
    # Create table
    cur.execute('''CREATE TABLE California_Zipcodes_AQI
                (Zipcode text,Air_Quality_Index real,Air_Quality_Index_Category text,Dominant_Pollutant text,
                 Carbon_Monoxide real,Nitrogen_Dioxide real, Ozone real,Inhalable_Particulate_Matter_pm10 real,
                 Fine_Particulate_Matter_pm25 real,Sulfur_Dioxide real,
                 FOREIGN KEY(Zipcode) REFERENCES California_Zipcodes_Geocodes(Zipcode))''')
    
    print("Table 'California_Zipcodes_AQI' created in DB 'Project_DataBase_California_Scraped_5' in the folder 'Dataset'!")
    print("Table 'California_Zipcodes_AQI' is used to store AQI data of all Zipcodes of California from API https://api.breezometer.com/air-quality")
    cur.execute("pragma table_info('California_Zipcodes_AQI')")    
    columns_data = cur.fetchall()
    col_names = []
    for val in columns_data:
            col_names.append(val[1])
    print("\nColumns in 'California_Zipcodes_AQI' table: ",col_names)      
    con.close()
    
#Insert 1 row
def AQI_table_insert_row(data):
    con = sqlite3.connect('Project_DataBase_California.db')
    cur = con.cursor()
    print(data)
    # Insert data
    try:
        cur.execute('INSERT or IGNORE INTO California_Zipcodes_AQI VALUES (?,?,?,?,?,?,?,?,?,?)', data)    
        # Save (commit) the changes
        con.commit()
    except:
        con.close()
        pass
    con.close()  
    
#Insert multiple rows
def AQI_table_insert_rows(dataset,con,cur):
    #con = sqlite3.connect('Project_DataBase_California.db')
    #cur = con.cursor()
    #print(dataset)
    # Insert data
    print("\nInserting AQI data in table 'California_Zipcodes_AQI' ! ")
    cur.executemany('INSERT or IGNORE INTO California_Zipcodes_AQI VALUES (?,?,?,?,?,?,?,?,?,?)', dataset)    
    # Save (commit) the changes
    con.commit()
    print("AQI data inserted in table 'California_Zipcodes_AQI' ! ")
    
def get_AQI_table_data(db_name,rows):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    sql_limit = (rows,)
    cur.execute("SELECT * FROM California_Zipcodes_AQI LIMIT ?",sql_limit)
    data = cur.fetchall()
    print('\nSample Data ('+str(rows)+' rows) from Table California_Zipcodes_AQI:\n') 
    
    for row in data:
        print(row)

    con.close() 

#Function for API call
def get_AQI_from_API(geocode):
    #List of pollutants to get pollutants concentrations
    pollutants = ["co","no2","o3","pm10","pm25", "so2"] #["co":"ppb","no2":"ppb","o3":"ppb","pm10":"ug/m3","pm25":"ug/m3", "so2":"ppb"]
    api_key = '1f52ec0b19c44402a20b1ae5671a3802' #school account
    payload = {'lat':geocode[1],'lon':geocode[2],'key':api_key}
    zip_geocode_url = 'https://api.breezometer.com/air-quality/v2/current-conditions?'
    
    #API request
    r = requests.get(zip_geocode_url,payload)
    content = r.content
    api_data = json.loads(content)
    aqi, aqi_category, dom_pollutant = 0,'',''
    #print(r.url)
    try:
        aqi = api_data['data']['indexes']['baqi']['aqi'] #AQI
        aqi_category = api_data['data']['indexes']['baqi']['category'] #AQI Category
        dom_pollutant = api_data['data']['indexes']['baqi']['dominant_pollutant'] #AQI
    except:pass
    result = (geocode[0],aqi,aqi_category,dom_pollutant)
    
    payload = {'lat':geocode[1],'lon':geocode[2],'key':api_key,'features':'pollutants_concentrations'}
    zip_geocode_url = 'https://api.breezometer.com/air-quality/v2/current-conditions?'
    
    #API request to get pollutants concentration
    r = requests.get(zip_geocode_url,payload)
    content = r.content
    api_data_poll = json.loads(content)
    polls_vals = []
    
    try:
        for poll in pollutants:   
            #Pollutants Concentrations data 
            polls_vals.append(api_data_poll['data']['pollutants'][poll]['concentration']['value'])
    except:
        polls_vals=['','','','','','']
        
    output = (result + tuple(polls_vals))  
    #print(r.url,output)
    
    return output

def get_zips_AQI(rows):
    
    if(isinstance(rows,int)):
        #For option --scrape
        AQI_table_create_5()
        DB_name = "Project_DataBase_California_Scraped_5.db"
        flag = 0
    else:
        AQI_table_create()
        DB_name = "Project_DataBase_California_Scraped.db"
        flag = 1
    path = 'Dataset/'    
    con = sqlite3.connect(path+DB_name)
    cur = con.cursor()
    
    cur.execute("SELECT COUNT(Zipcode) FROM California_Zipcodes_Geocodes")
    total_rows = cur.fetchone()[0]
    offset = 0
    
    if(flag == 0):
            total_rows = rows
            
    while offset<total_rows:
        sql_offset = (offset,)
        #Get geocodes from database and use it to get AQI 
        if flag == 1: 
            cur.execute("SELECT * FROM California_Zipcodes_Geocodes ORDER BY Zipcode LIMIT 100 OFFSET ?",sql_offset)
            
        else: 
            sql = "SELECT * FROM California_Zipcodes_Geocodes ORDER BY Zipcode LIMIT " + str(rows) +" OFFSET ?"
            cur.execute(sql,sql_offset)
            
        geocodes = cur.fetchall()
        AQI_data =[]
        print("\n\nFetching AQI data of zipcodes from API..")
        for geocode in geocodes:
            result = get_AQI_from_API(geocode)
            AQI_data.append(result)
        print("AQI data fetched from API!")
        
        #Insert into AQI table
        AQI_table_insert_rows(AQI_data,con,cur)          
        offset+=100    
    con.close()
