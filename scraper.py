#!/usr/bin/env python
# coding: utf-8

# In[1]:


import urllib
import requests
import json
import time
import sqlite3
import xml.etree.ElementTree as ET
from bs4 import element, BeautifulSoup
import argparse

# For option --static <path>
def get_static_data(path):
    con = sqlite3.connect(path+'Project_DataBase_California.db')
    cur = con.cursor()
    
    cur.execute("SELECT * FROM sqlite_master WHERE type='table'")
    
    tables = cur.fetchall()
    
    print("There are 4 tables being used in this project: \n")
    
    for i in range(4):
            table_name = tables[i][1]
            print(table_name) 
            
    for i in range(4):
        table_name = tables[i][1]
        
        print("\n\nTable ",i+1,": ",table_name) 
        
        print("\n\nTable ",i+1,table_name," Stats")
        cur.execute("Select count(*) from "+table_name)        
        res1 = cur.fetchone()
        print("Number of rows in table: ",res1[0])
        
        cur.execute("pragma table_info('"+table_name+"')")    
        columns_data = cur.fetchall()
        col_names = []
        print ("{:<22}{:<30}{}".format("\nColumn ID  |","  Column Name\t     |","Column Data Type"))
        for val in columns_data:
            print ("{:<12}{:<48}{}".format(val[0],val[1],val[2]))
            col_names.append(val[1])
            
        
        print("\n\nTable ",i+1,table_name," :Sample Data") 
        
        cur.execute("Select * from "+table_name+" LIMIT 5")
        
        output = cur.fetchall()
        print("Table Columns Names:\n",col_names,"\n\n5 rows from Table:")
        for row in output:
            print(row)
        

    con.close()
    return tables

# - scraper.py --scrape (This will scrape it but, get only 5 entries of each dataset)

def scrape_5():
    from Scrape_California_Zipcodes import scrape_zips,get_zipcode_table_data
    from Scrape_California_Geocodes import get_zips_geocodes,get_geocodes_table_data
    from Scrape_California_AQI import get_zips_AQI,get_AQI_table_data
    from Scrape_California_Vehicles import get_zips_vehicles_data,get_vehicles_table_data
    
    print("Dataset 1")
    print("=========")
    scrape_zips(5)
    get_zipcode_table_data('Dataset/Project_DataBase_California_Scraped_5.db',5)
    print("------------------------------------------------------------------------------------------------")
    
    print("\nDataset 2")
    print("=========")
    get_zips_geocodes(5)
    get_geocodes_table_data('Dataset/Project_DataBase_California_Scraped_5.db',5)
    print("------------------------------------------------------------------------------------------------")
    
    print("\nDataset 3")
    print("=========")    
    get_zips_AQI(5)
    get_AQI_table_data("Dataset/Project_DataBase_California_Scraped_5.db",5)
    print("------------------------------------------------------------------------------------------------")
    
    print("\nDataset 4")
    print("=========")    
    get_zips_vehicles_data(5)
    get_vehicles_table_data("Dataset/Project_DataBase_California_Scraped_5.db",5)
    

# - scraper.py (Scrape whole datasets)
def scrape_whole():
    from Scrape_California_Zipcodes import scrape_zips,get_zipcode_table_data
    from Scrape_California_Geocodes import get_zips_geocodes,get_geocodes_table_data
    from Scrape_California_AQI import get_zips_AQI,get_AQI_table_data
    from Scrape_California_Vehicles import get_zips_vehicles_data,get_vehicles_table_data
    
    print("Dataset 1")
    print("=========")
    scrape_zips('all')
    get_zipcode_table_data('Dataset/Project_DataBase_California_Scraped.db',5)
    print("------------------------------------------------------------------------------------------------")
    
    print("\nDataset 2")
    print("=========")
    get_zips_geocodes('all')
    get_geocodes_table_data('Dataset/Project_DataBase_California_Scraped.db',5)
    print("------------------------------------------------------------------------------------------------")
    
    print("\nDataset 3")
    print("=========")    
    get_zips_AQI('all')
    get_AQI_table_data("Dataset/Project_DataBase_California_Scraped.db",5)
    print("------------------------------------------------------------------------------------------------")
    
    print("\nDataset 4")
    print("=========")    
    get_zips_vehicles_data('all')
    get_vehicles_table_data("Dataset/Project_DataBase_California_Scraped.db",5)
    

#Get user input
parser = argparse.ArgumentParser()
parser.add_argument("--scrape",action='store_true', help="To scrape 5 entries                                                            of the dataset and view 5 rows")
parser.add_argument("--static",type=str, help="To view stats and sample data of static tables in Database.")
args = parser.parse_args()

if args.scrape:
    #Scrape 5 entries of each dataset and print
    print("Option Chosen: --scrape")
    print("The program will scrape 5 entries from each dataset and print 5 rows.")
    scrape_5()
elif args.static:
    #Print 5 rows of each dataset
    print("Option Chosen: --static")
    print("The program will print stats and 5 rows from each static dataset.")
    get_static_data(args.static)
else:
    #Scrape whole datasets
    print("Option Chosen:Scrape whole")
    print("The program will scrape whole datasets and print 5 sample rows.")
    scrape_whole()

