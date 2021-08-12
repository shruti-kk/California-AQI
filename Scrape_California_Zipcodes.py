#!/usr/bin/env python
# coding: utf-8

# In[30]:


import urllib
import requests
import json
import time
import sqlite3
import pandas as pd
import xml.etree.ElementTree as ET
from bs4 import element
from bs4 import BeautifulSoup

# SQLite table for ZIPCODES
#create table
def zipcode_table_create():
    con = sqlite3.connect('Dataset/Project_DataBase_California_Scraped.db')
    cur = con.cursor()
    # Drop table
    cur.executescript('DROP TABLE if exists California_Zipcodes')
    # Create table
    cur.execute('''CREATE TABLE California_Zipcodes
                (Zipcode text primary key,Zipcode_URL text,County_Name text,Total_Population real,Population_in_Households
                real,Population_in_Families real,Population_in_Group_Quarters1 real,Population_Density real,
                Diversity_Index2 real,Median_Household_Income real,Average_Household_Income real,
                Percentage_of_Income_for_Mortgage4 real,
                Per_Capita_Income real,Wealth_Index5 real,Total_Housing_Units real,Owner_Occupied_HU real,
                Renter_Occupied_HU real,Vacant_Housing_Units real,Median_Home_Value real,Average_Home_Value real,
                Housing_Affordability_Index3 real,Total_Households real,Average_Household_Size real,Family_Households real,
                Average_Family_Size real)''')
    
       
    print("Table 'California_Zipcodes' created in DB 'Project_DataBase_California_Scraped' in the folder 'Dataset'!")
    print("Table 'California_Zipcodes' is used to store all Zipcodes of California scraped from https://california.hometownlocator.com/zip-codes/counties.cfm")
    cur.execute("pragma table_info('California_Zipcodes')")    
    columns_data = cur.fetchall()
    col_names = []
    for val in columns_data:
            col_names.append(val[1])
    print("\nColumns in 'California_Zipcodes' table: ",col_names)    
    con.close() 
    
#create table to store data for --scrape option    
def zipcode_table_create_5():
    con = sqlite3.connect('Dataset/Project_DataBase_California_Scraped_5.db')
    cur = con.cursor()
    # Drop table
    cur.executescript('DROP TABLE if exists California_Zipcodes')
    # Create table
    cur.execute('''CREATE TABLE California_Zipcodes
                (Zipcode text primary key,Zipcode_URL text,County_Name text,Total_Population real,Population_in_Households
                real,Population_in_Families real,Population_in_Group_Quarters1,Population_Density real,
                Diversity_Index2 real,Median_Household_Income real,Average_Household_Income,
                Percentage_of_Income_for_Mortgage4 real,
                Per_Capita_Income real,Wealth_Index5 real,Total_Housing_Units real,Owner_Occupied_HU real,
                Renter_Occupied_HU real,Vacant_Housing_Units real,Median_Home_Value real,Average_Home_Value,
                Housing_Affordability_Index3 real,Total_Households real,Average_Household_Size real,Family_Households real,
                Average_Family_Size real)''')
    print("Table 'California_Zipcodes'created in DB 'Project_DataBase_California_Scraped_5' in the folder 'Dataset'! ")
    print("Table 'California_Zipcodes' is used to store all Zipcodes of California scraped from https://california.hometownlocator.com/zip-codes/counties.cfm")
    cur.execute("pragma table_info('California_Zipcodes')")  
    columns_data = cur.fetchall()
    col_names = []
    for val in columns_data:
            col_names.append(val[1])
    print("\n\nColumns in 'California_Zipcodes' table: ",col_names)      
    con.close()
    
#Insert 1 row    
def zipcode_table_insert_row(data,db_name):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    # Insert data
    try:
        cur.execute('INSERT or IGNORE INTO California_Zipcodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', data)    
        # Save (commit) the changes
        con.commit()
    except:
        con.close()
        pass
    con.close()

#Insert multiple rows
def zipcode_table_insert_rows(dataset,db_name):
    print(db_name)
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    print("Inserting Zipcodes data in table 'California_Zipcodes'..")
    # Insert data
    cur.executemany('INSERT or IGNORE INTO California_Zipcodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', dataset)    
    # Save (commit) the changes
    con.commit()        
    con.close()
    print("Zipcodes data inserted in table 'California_Zipcodes' ! ")
    
    
def get_zipcode_table_data(db_name,rows):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    sql_limit = (rows,)
    cur.execute("SELECT * FROM California_Zipcodes LIMIT ?",sql_limit)
    zipcodes = cur.fetchall()
    print('\nSample Data ('+str(rows)+' rows) from Table California_Zipcodes:\n') 
    
    for zipcode in zipcodes:
        print(zipcode)

    con.close()  

#Get data as Dataframe
def get_zipcode_DF(db_name,query):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    cur.execute(query)
    output = cur.fetchall()
    cols = [column[0] for column in cur.description]    
    con.close() 
    results = pd.DataFrame.from_records(data = output, columns = cols)    
    return results    
    
#Get data using query
def get_zipcode_data(db_name,query):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    cur.execute(query)
    output = cur.fetchall()
    con.close() 
    
    return output



#Scrape Demographic Data of each zipcode
def scrape_zip_demographic_data(zip_code_url):
    zip_data_url = "https://california.hometownlocator.com/zip-codes/"+zip_code_url
    zip_data_r = requests.get(zip_data_url)
    zip_data_soup = BeautifulSoup(zip_data_r.content, 'html.parser')
    zip_data ={}
    
    tables = ['POPULATION','HOUSING','INCOME','HOUSEHOLDS']
    for ind in range(len(tables)):
        try:
            tag = zip_data_soup.find('h4',text=tables[ind]).findNext('tr')
        except:
            continue
        data = ''
        while True:
            if tag.find("h4") !=None :
                break
            if tag.find("td").text.isspace():
                break
            elif tag.name == 'tr':
                tag = tag.findNext('td')
                field = tag.text
                #tag = tag.findNext()
                tag = tag.findNext('td')
                val = tag.text
                zip_data[field] = val.split()[0].replace('$',"").replace(',',"").replace('%',"")
            tag = tag.findNext('tr')
            
    return zip_data
    
#Scrape zipcodes of each county webpage
def scrape_county_zips(zip_url_ext,county_name,rows):    
    zip_data_cols = ['Total Population', 'Population in Households', 'Population in Families', 'Population in Group Quarters1',           'Population Density', 'Diversity Index2', 'Median Household Income', 'Average Household Income',           '% of Income for Mortgage4', 'Per Capita Income', 'Wealth Index5', 'Total HU (Housing Units)',            'Owner Occupied HU', 'Renter Occupied HU', 'Vacant Housing Units', 'Median Home Value', 'Average Home Value',           'Housing Affordability Index3', 'Total Households', 'Average Household Size', 'Family Households',            'Average Family Size']
    
    zip_url = "https://california.hometownlocator.com" + zip_url_ext    
    r = requests.get(zip_url)
    soup = BeautifulSoup(r.content, 'html.parser')
    zips = soup.select("a[href^='data,zipcode,']")#starts with
    #soup.select_one("a[href*=location]")#contains
    counties_zips_data = []
    flag = 0
    i=0
    if isinstance(rows,int):
        flag = 1
        
    for zipcode in zips:
        
        if flag == 1:
            if i>=rows:
                break
            
        zip_code_url = zipcode['href']
        zip_data = scrape_zip_demographic_data(zip_code_url)          

        data =[zipcode.text,"https://california.hometownlocator.com/zip-codes/"+zip_code_url,county_name]
        for col in zip_data_cols:
            col_val = zip_data.get(col,0)
            data.append(col_val)
        counties_zips_data.append(tuple(data))
        i+=1
    return counties_zips_data

#Scrape all county names and hyperlinks from counties webpage
def scrape_counties():
    print("\n\nScraping Counties from https://california.hometownlocator.com/zip-codes/counties.cfm ...")
    counties_url = "https://california.hometownlocator.com/zip-codes/counties.cfm"
    r = requests.get(counties_url)
    counties_soup = BeautifulSoup(r.content, 'html.parser')
    counties = counties_soup.select("a[href^='/zip-codes/countyZIPS,scfips']") #starts with
    
    return counties

def scrape_zips(rows):
    path = 'Dataset/'
    if(isinstance(rows,int)):
        zipcode_table_create_5()
        DB_name = "Project_DataBase_California_Scraped_5.db"
    else:
        zipcode_table_create()
        DB_name = "Project_DataBase_California_Scraped.db"
        
    cols = "Zipcode","ZipcodeURL","CountyName","TotalPopulation","PopulationDensity"      
    county_zips = []
    counties = scrape_counties()
    print("Counties data scraping completed!")
    
    for county in counties:
        print("Scraping Zipcodes and Zipcode Demographic Data...")
        zip_url_ext = county['href']
        county_zips = scrape_county_zips(zip_url_ext,county.text,rows)
        print("Zipcodes and Zipcode Demographic Data scraping completed!")
        zipcode_table_insert_rows(county_zips,path+DB_name)
        
        if(isinstance(rows,int)):
                break
