#!/usr/bin/env python
# coding: utf-8

# In[17]:


import urllib
import requests
import json
import time
import sqlite3
import pandas as pd
import xml.etree.ElementTree as ET
from bs4 import element
from bs4 import BeautifulSoup

# SQLite table for Zipcodes Geocodes
#create table
def geocodes_table_create():
    con = sqlite3.connect('Dataset/Project_DataBase_California_Scraped.db')
    con.execute("PRAGMA foreign_keys = 1")
    cur = con.cursor()
    # Drop table
    cur.executescript('DROP TABLE if exists California_Zipcodes_Geocodes')
    # Create table
    cur.execute('''CREATE TABLE California_Zipcodes_Geocodes
                (Zipcode text,Zipcode_Latitude real,Zipcode_Longitude real,
                FOREIGN KEY(Zipcode) REFERENCES California_Zipcodes(Zipcode))''')
    
    print("Table 'California_Zipcodes_Geocodes' created in DB 'Project_DataBase_California_Scraped' in the folder 'Dataset'!")
    print("Table 'California_Zipcodes_Geocodes' is used to store Geocodes of all Zipcodes of California from API http://www.mapquestapi.com")
    cur.execute("pragma table_info('California_Zipcodes_Geocodes')")    
    columns_data = cur.fetchall()
    col_names = []
    for val in columns_data:
            col_names.append(val[1])
    print("\nColumns in 'California_Zipcodes_Geocodes' table: ",col_names)
    con.close()

#create table to store data for --scrape option 
def geocodes_table_create_5():
    con = sqlite3.connect('Dataset/Project_DataBase_California_Scraped_5.db')
    cur = con.cursor()
    # Drop table
    cur.executescript('DROP TABLE if exists California_Zipcodes_Geocodes')
    # Create table
    cur.execute('''CREATE TABLE California_Zipcodes_Geocodes
                (Zipcode text,Zipcode_Latitude real,Zipcode_Longitude real,
                FOREIGN KEY(Zipcode) REFERENCES California_Zipcodes(Zipcode))''')
    
    print("Table 'California_Zipcodes_Geocodes' created in DB 'Project_DataBase_California_Scraped_5' in the folder 'Dataset'!")
    print("Table 'California_Zipcodes_Geocodes' is used to store Geocodes of all Zipcodes of California from API http://www.mapquestapi.com")
    cur.execute("pragma table_info('California_Zipcodes_Geocodes')")    
    columns_data = cur.fetchall()
    col_names = []
    for val in columns_data:
            col_names.append(val[1])
    print("\nColumns in 'California_Zipcodes_Geocodes' table: ",col_names)      
    con.close()
    
#Insert 1 row
def geocodes_table_insert_row(data,db_name):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    # Insert data
    try:
        cur.execute('INSERT or IGNORE INTO California_Zipcodes_Geocodes VALUES (?,?,?)', data)    
        # Save (commit) the changes
        con.commit()
    except:
        con.close()
        pass
    con.close()
  
    
#Insert multiple rows
def geocodes_table_insert_rows(data,db_name):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    print("\nInserting Geocodes data in table 'California_Zipcodes_Geocodes' ! ")
    # Insert data
    cur.executemany('INSERT or IGNORE INTO California_Zipcodes_Geocodes VALUES (?,?,?)', data)    
    # Save (commit) the changes
    con.commit()        
    con.close()
    print("Geocodes data inserted in table 'California_Zipcodes_Geocodes' ! ")
    
def get_geocodes_table_data(db_name,rows):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    sql_limit = (rows,)
    cur.execute("SELECT * FROM California_Zipcodes_Geocodes LIMIT ?",sql_limit)
    geocodes = cur.fetchall()
    print('\nSample Data ('+str(rows)+' rows) from Table California_Zipcodes_Geocodes:\n') 
    
    for geocode in geocodes:
        print(geocode)

    con.close()    

#Function for API call
def get_geocodes_from_API(zipcode):
    zip_geocode_url = 'http://www.mapquestapi.com/geocoding/v1/address?key=%20Fk3KbxR5YbdtsJVC6pgzeaGd4MYRt4zA&location=' + zipcode
    
    #API request
    r = requests.get(zip_geocode_url)
    content = r.content
    api_data = json.loads(content)
    lat, lng = 0,0
    try:
        lat, lng = api_data['results'][0]['locations'][0]['latLng']['lat'],                    api_data['results'][0]['locations'][0]['latLng']['lng']

    except:pass   
    return (zipcode,lat,lng)

def get_zips_geocodes(rows):
    path = 'Dataset/'
    if(isinstance(rows,int)):
        geocodes_table_create_5()
        DB_name = "Project_DataBase_California_Scraped_5.db"
    else:
        geocodes_table_create()
        DB_name = "Project_DataBase_California_Scraped.db"
     
    #Fetch list of zipcodes from DB    
    con = sqlite3.connect(path+DB_name)
    cur = con.cursor()   
    cur.execute("SELECT Zipcode FROM California_Zipcodes")
    zipcodes = cur.fetchall()
    con.close()
    
    zipcodes_geocodes =[]  
    #Get geocodes for each zipcode
    print("Fetching geocodes of zipcodes from API..")
    for zipcode in zipcodes:
        result = get_geocodes_from_API(zipcode[0])
        zipcodes_geocodes.append(result)
    
    print("Geocodes from API fetched!") 
    
    #Insert geocode data
    geocodes_table_insert_rows(zipcodes_geocodes,path+DB_name)




