#!/usr/bin/env python
# coding: utf-8

#Libraries for general purpose
import urllib
import requests
import json
import time
import sqlite3
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from bs4 import element, BeautifulSoup
import argparse
import webbrowser, os

#Linear and logistic regression libraries
from sklearn.linear_model import LinearRegression,LogisticRegression
from sklearn.metrics import r2_score
import statsmodels.api as sm

#Visualization libraries
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns
import plotly.figure_factory as ff

#Scraping python scripts imported
from Scrape_California_Zipcodes import scrape_zips,get_zipcode_table_data,get_zipcode_data
from Scrape_California_Geocodes import get_zips_geocodes,get_geocodes_table_data
from Scrape_California_AQI import get_zips_AQI,get_AQI_table_data
from Scrape_California_Vehicles import get_zips_vehicles_data,get_vehicles_table_data

# For option --static 
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

#Get database data as Dataframe
def get_database_DF(db_name,query):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    cur.execute(query)
    output = cur.fetchall()
    cols = [column[0] for column in cur.description]    
    con.close() 
    results = pd.DataFrame.from_records(data = output, columns = cols)    
    return results    

#Merge the 4 databses in required dataframe format
def merge_data():
    print("Fetch the data of the 4 tables using SQL join operation..")
    db_name = "Dataset/Project_DataBase_California.db"
    dataset = get_database_DF(db_name,'''SELECT * FROM California_Zipcodes 
                JOIN California_Zipcodes_Geocodes ON California_Zipcodes.Zipcode=California_Zipcodes_Geocodes.Zipcode 
                JOIN California_Zipcodes_AQI ON California_Zipcodes.Zipcode=California_Zipcodes_AQI.Zipcode''')
    dataset = dataset.loc[:,~dataset.columns.duplicated()]

    vehicles_DF = get_database_DF(db_name,"SELECT * FROM California_Zipcodes_Vehicles ORDER BY Zip_Code")
    vehicles_DF = vehicles_DF[vehicles_DF['Zip_Code'].isin(dataset['Zipcode'])]
    
    #Merge Duty to dataset DF
    duty = vehicles_DF.groupby(['Zip_Code','Duty'])['Vehicles'].sum().unstack(fill_value=0).reset_index()
    dataset = dataset.merge(duty, left_on='Zipcode', right_on='Zip_Code', how='left')
    dataset = dataset.drop('Zip_Code', 1)

    #Merge Fuel Type to dataset DF
    fuel = vehicles_DF.groupby(['Zip_Code','Fuel'])['Vehicles'].sum().unstack(fill_value=0).reset_index()
    fuel1 = pd.DataFrame(fuel['Zip_Code'])
    fuel1['Hybrid_Vehicles'] = fuel[['Diesel and Diesel Hybrid','Hybrid Gasoline','Plug-in Hybrid']].sum(axis=1) #Hybrid Vehicles
    fuel1['Electric_Vehicles']=fuel['Battery Electric']
    fuel1['Fuel_Vehicles'] = fuel[['Flex-Fuel','Gasoline','Hydrogen Fuel Cell','Natural Gas']].sum(axis=1)

    dataset = dataset.merge(fuel1, left_on='Zipcode', right_on='Zip_Code', how='left')
    dataset = dataset.drop('Zip_Code', 1)
    
    #Merge Year Type to dataset DF
    year = vehicles_DF.groupby(['Zip_Code','Model_Year'])['Vehicles'].sum().unstack(fill_value=0).reset_index()
    year1 = pd.DataFrame(year['Zip_Code'])
    year1['<=2010'] = year[['<2007',2008,2009,2010]].sum(axis=1)
    year1['2010-2015']=year[[2011, 2012,2013,2014,2015]].sum(axis=1)
    year1['2016-2020'] = year[[2016,2017, 2018,2019,2020]].sum(axis=1)
    dataset = dataset.merge(year1, left_on='Zipcode', right_on='Zip_Code', how='left')
    dataset = dataset.drop('Zip_Code', 1)    
    
    print("Merged data to the required format into Pandas Dataframe with rows as zipcodes!")
    return dataset

#Merge the 4 databses in required dataframe format
def merge_data_scrape():
    print("Fetch the data of the 4 tables using SQL join operation..")
    db_name = "Dataset/Project_DataBase_California_Scraped.db"
    dataset = get_database_DF(db_name,'''SELECT * FROM California_Zipcodes 
                JOIN California_Zipcodes_Geocodes ON California_Zipcodes.Zipcode=California_Zipcodes_Geocodes.Zipcode 
                JOIN California_Zipcodes_AQI ON California_Zipcodes.Zipcode=California_Zipcodes_AQI.Zipcode''')
    dataset = dataset.loc[:,~dataset.columns.duplicated()]

    vehicles_DF = get_database_DF(db_name,"SELECT * FROM California_Zipcodes_Vehicles ORDER BY Zip_Code")
    vehicles_DF = vehicles_DF[vehicles_DF['Zip_Code'].isin(dataset['Zipcode'])]
    
    #Merge Duty to dataset DF
    duty = vehicles_DF.groupby(['Zip_Code','Duty'])['Vehicles'].sum().unstack(fill_value=0).reset_index()
    dataset = dataset.merge(duty, left_on='Zipcode', right_on='Zip_Code', how='left')
    dataset = dataset.drop('Zip_Code', 1)

    #Merge Fuel Type to dataset DF
    fuel = vehicles_DF.groupby(['Zip_Code','Fuel'])['Vehicles'].sum().unstack(fill_value=0).reset_index()
    fuel1 = pd.DataFrame(fuel['Zip_Code'])
    fuel1['Hybrid_Vehicles'] = fuel[['Diesel and Diesel Hybrid','Hybrid Gasoline','Plug-in Hybrid']].sum(axis=1) #Hybrid Vehicles
    fuel1['Electric_Vehicles']=fuel['Battery Electric']
    fuel1['Fuel_Vehicles'] = fuel[['Flex-Fuel','Gasoline','Hydrogen Fuel Cell','Natural Gas']].sum(axis=1)

    dataset = dataset.merge(fuel1, left_on='Zipcode', right_on='Zip_Code', how='left')
    dataset = dataset.drop('Zip_Code', 1)
    
    #Merge Year Type to dataset DF
    year = vehicles_DF.groupby(['Zip_Code','Model_Year'])['Vehicles'].sum().unstack(fill_value=0).reset_index()
    year1 = pd.DataFrame(year['Zip_Code'])
    year1['<=2010'] = year[['<2007',2008,2009,2010]].sum(axis=1)
    year1['2010-2015']=year[[2011, 2012,2013,2014,2015]].sum(axis=1)
    year1['2016-2020'] = year[[2016,2017, 2018,2019,2020]].sum(axis=1)
    dataset = dataset.merge(year1, left_on='Zipcode', right_on='Zip_Code', how='left')
    dataset = dataset.drop('Zip_Code', 1)    
    
    print("Merged data to the required format into Pandas Dataframe with rows as zipcodes!")
    return dataset

def clean_data(dataset):
    df_1000 = dataset.copy()
    df_1000 = df_1000[df_1000.Total_Population != 0]
    cols = ['Heavy','Light','Hybrid_Vehicles','Electric_Vehicles','Fuel_Vehicles','<=2010','2010-2015','2016-2020']
    print("Clean the data by converting absolute vehicular data to per 100 people vales.")
    for col in cols:
        df_1000[col+"_Per_1000"] = df_1000.apply(lambda x: (x[col] * 1000) / x['Total_Population'], axis = 1)
        
    return df_1000


def get_genrl_stats():
    print("The state of California has %d counties." %(len(set(dataset["County_Name"]))))
    print("Total number of Zipcodes covered for analysis:",len(dataset["Zipcode"]))
    print("Following are some of the demographic stats:")
    #Describe few stats of the datatset
    print(df_per_1000[['Total_Population',
           'Population_in_Households','Average_Household_Size','Median_Household_Income',
           'Average_Household_Income','Per_Capita_Income']].describe())

    #Describe few stats of the datatset
    print("Following are some of the AQI stats:")
    print(dataset[['Air_Quality_Index_Category', 'Dominant_Pollutant', 'Carbon_Monoxide',
           'Nitrogen_Dioxide', 'Ozone', 'Inhalable_Particulate_Matter_pm10',
           'Fine_Particulate_Matter_pm25', 'Sulfur_Dioxide']].describe())

    #Describe few stats of the datatset
    print("Following are some of the Vehicular data stats:")
    print(dataset[['Heavy', 'Light',
           'Hybrid_Vehicles', 'Electric_Vehicles', 'Fuel_Vehicles', '<=2010',
           '2010-2015', '2016-2020']].describe())


def map_hover(fig,data):
    import warnings
    warnings.filterwarnings('ignore')
    hover_ix, hover = [(ix, t) for ix, t in enumerate(fig['data']) if t.text][0]
    # mismatching lengths indicates bug
    if len(hover['text']) != len(data):

        ht = pd.Series(hover['text'])

        no_dupe_ix = ht.index[~ht.duplicated()]

        hover_x_deduped = np.array(hover['x'])[no_dupe_ix]
        hover_y_deduped = np.array(hover['y'])[no_dupe_ix]

        new_hover_x = [x if type(x) == float else x[0] for x in hover_x_deduped]
        new_hover_y = [y if type(y) == float else y[0] for y in hover_y_deduped]

        fig['data'][hover_ix]['text'] = ht.drop_duplicates()
        fig['data'][hover_ix]['x'] = new_hover_x
        fig['data'][hover_ix]['y'] = new_hover_y
        
        return fig
    
def analysis(dataset,vehicles_DF):
    
    #############################
    #AQI category distribution
    print("\n\nAir Quality Index Category Distribution using Bar Plot..")
    res = dataset.groupby("Air_Quality_Index_Category")["Air_Quality_Index_Category"].size().sort_values(ascending=False)
    res.index = ['Good','Moderate', 'Low','Excellent']
    # Draw plot
    fig, ax = plt.subplots(figsize=(5,5), facecolor='white', dpi= 80)
    plt.bar(res.index, res, color = '#47B39C')
    # Annotate Text
    txt = res
    for i, cty in enumerate(txt):    
        ax.text(i, cty+15, '{}%'.format(round((cty/txt.sum())*100, 1)), horizontalalignment='center')
    # Title, Label, Ticks and Ylim
    ax.set_title('Air Quality Index Category Distribution', fontdict={'size':12})
    ax.set(ylabel='Count')    
    print("\nVizualization can be found in folder *visualizations*. Filename:1_AQI_Category_Distribution.png")
    print("----------------------------------------------------------------------------------------------")
    plt.show()
    #############################
    
    #Counties with lowest average AQI 
    print("\n\nBar Chart of Air Quality Index by County..")
    res = dataset.groupby("County_Name")["Air_Quality_Index"].mean().sort_values()

    # Draw plot
    fig, ax = plt.subplots(figsize=(20,5), facecolor='white', dpi= 80)
    plt.bar(res.index[:10], res[:10], color = '#EC6B56')
    plt.bar(res.index[30:40], res[30:40], color = '#FFC154')
    plt.bar(res.index[-10:], res[-10:], color = '#47B39C')
    my_colors = 'rgb'
    # Annotate Text
    txt = res[:10].append(res[30:40]).append(res[-10:])
    for i, cty in enumerate(txt):
        ax.text(i, cty+0.5, round(cty, 1), horizontalalignment='center')


    # Title, Label, Ticks and Ylim
    xtick = res.index[:10].append(res.index[30:40]).append(res.index[-10:])
    ax.set_title('Bar Chart of Air Quality Index by County', fontdict={'size':15})
    ax.set(ylabel='Mean Air Quality Index', ylim=(0,80))
    plt.xticks(xtick, xtick.str.upper(), rotation=70, horizontalalignment='right', fontsize=12)
    plt.show()
    print("\nVizualization can be found in folder *visualizations*. Filename:2_AQI_County_Distribution.png")
    print("----------------------------------------------------------------------------------------------")
    #############################

    print("\n\nCorrelation Matrix of Numerical Fatures..")
    #Correlation Matrix
    remove = ["Zipcode",'Zipcode','Zipcode_Latitude','Zipcode_Longitude','Zipcode','Zipcode_URL','County_Name',
              'Owner_Occupied_HU','Renter_Occupied_HU','Vacant_Housing_Units','Median_Home_Value','Average_Home_Value',
              'Housing_Affordability_Index3','Population_in_Households','Population_in_Families','Population_in_Group_Quarters1',
              'Population_Density','Diversity_Index2','Median_Household_Income','Population_in_Households',
              'Population_in_Families','Population_in_Group_Quarters1','Percentage_of_Income_for_Mortgage4',
              'Housing_Affordability_Index3','Family_Households','Average_Family_Size','Wealth_Index5', 'Total_Housing_Units']
    #'Heavy', 'Light', 'Hybrid_Vehicles', 'Electric_Vehicles', 'Fuel_Vehicles', '<=2010', '2010-2015', '2016-2020',]
    features = list(dataset.columns)
    corr_features = [col for col in features if col not in remove]
    corr_features
    df= dataset[corr_features]
    fig, ax = plt.subplots(figsize=(12, 10)) 
    mask = np.zeros_like(df.corr())
    mask[np.triu_indices_from(mask)] = 1
    sns.heatmap(df.corr(), mask= mask, ax= ax, annot= True,fmt=".2f",cmap= 'Blues')
    #fig.savefig('visualizations/3_Corr_Matrix.png')
    print("\nVizualization can be found in folder *visualizations*. Filename:3_Corr_Matrix.png")
    print("----------------------------------------------------------------------------------------------")    
    plt.show()
    #############################

    print("\n\nRegression plot for Per_Capita_Income and Total_Population against Air_Quality_Index..")
    #Regression plot for Per_Capita_Income and Total_Population against Air_Quality_Index
    fig, axs = plt.subplots(ncols=2)
    fig.set_size_inches(10, 5)
    #Remove Outliers
    x = df_per_1000[((df_per_1000['Per_Capita_Income'] - df_per_1000['Per_Capita_Income'].mean()) / df_per_1000['Per_Capita_Income'].std()).abs() < 4]
    sns.regplot(x='Per_Capita_Income', y='Air_Quality_Index', data=x, ax=axs[0],scatter_kws={"color": "#EC6B56",'s':3},            line_kws={"color": "#EC6B56"})
    sns.regplot(x='Total_Population', y='Air_Quality_Index', data=df_per_1000, ax=axs[1],scatter_kws={"color": "#EC6B56",'s':3},            line_kws={"color": "#EC6B56"})
    plt.show()
    #fig.savefig('visualizations/4_Regplot_Pop_Income.png')
    print("\nVizualization can be found in folder *visualizations*. Filename:4_Regplot_Pop_Income.png")
    print("----------------------------------------------------------------------------------------------")
    #############################

    print("\n\nRegression plot for change in AQI wrt Vehicles fuel type..")
    #Regression plot for change in AQI wrt Vehicles fuel type
    fig, axs = plt.subplots(ncols=3)
    fig.set_size_inches( 15, 8)

    df = df_per_1000
    #Remove outliers
    df = df.drop(df.sort_values('Fuel_Vehicles_Per_1000')['Fuel_Vehicles_Per_1000'][-6:].index)
    df = df.drop(df.sort_values('Hybrid_Vehicles_Per_1000')['Hybrid_Vehicles_Per_1000'][-5:].index)
    df = df.drop(df.sort_values('Electric_Vehicles_Per_1000')['Electric_Vehicles_Per_1000'][-5:].index)
    sns.regplot(x='Electric_Vehicles_Per_1000', y='Air_Quality_Index', data=df, ax=axs[0],            scatter_kws={"color": "#47B39C",'s':20},            line_kws={"color": "#47B39C"})
    sns.regplot(x='Hybrid_Vehicles_Per_1000', y='Air_Quality_Index', data=df, ax=axs[1],            scatter_kws={"color": "#47B39C",'s':20},            line_kws={"color": "#47B39C"})
    sns.regplot(x='Fuel_Vehicles_Per_1000', y='Air_Quality_Index', data=df, ax=axs[2],            scatter_kws={"color": "#47B39C",'s':20},            line_kws={"color": "#47B39C"})
    #fig.savefig('visualizations/5_Regplot_Vehicles.png')
    print("\nVizualization can be found in folder *visualizations*. Filename:5_Regplot_Vehicles.png")
    print("----------------------------------------------------------------------------------------------")
    plt.show()
    #############################
    
    #Compare vehicles of the top 5 and bottom 5 Zipcodes sorted by AQI
    print("\n\nCompare vehicles of Zipcodes with highest and lowest AQI and visualize using Boxplot")
    df = df_per_1000.copy()
    df = df[df.Total_Population != 0]
    df = df.drop(df.sort_values('Fuel_Vehicles_Per_1000')['Fuel_Vehicles_Per_1000'][-6:].index)
    df = df.drop(df.sort_values('Hybrid_Vehicles_Per_1000')['Hybrid_Vehicles_Per_1000'][-7:].index)
    df = df.drop(df.sort_values('Electric_Vehicles_Per_1000')['Electric_Vehicles_Per_1000'][-5:].index)

    AQI_sort = df.sort_values(by=['Air_Quality_Index'], ascending=True)
    #Best AQI - 5 Zipcodes
    AQI_highest = AQI_sort[-5:]
    #Worst AQI - 5 Zipcodes
    AQI_least = AQI_sort[:5]

    fig, axes = plt.subplots(nrows=3,ncols=2,figsize=(12,6))
    ax = AQI_highest.boxplot(ax=axes[0][0], column = ['Hybrid_Vehicles_Per_1000', 'Electric_Vehicles_Per_1000']).set_title('Top 5 Zipcodes')
    AQI_least.boxplot(ax=axes[0][1], column = ['Hybrid_Vehicles_Per_1000', 'Electric_Vehicles_Per_1000']).set_title('Bottom 5 Zipcodes')
    AQI_highest.boxplot(ax=axes[1][0], column = ['Fuel_Vehicles_Per_1000'])
    AQI_least.boxplot(ax=axes[1][1], column = ['Fuel_Vehicles_Per_1000'])
    ax = AQI_highest.boxplot(ax=axes[2][0], column = ['<=2010_Per_1000', '2010-2015_Per_1000','2016-2020_Per_1000'])
    AQI_least.boxplot(ax=axes[2][1], column = ['<=2010_Per_1000', '2010-2015_Per_1000','2016-2020_Per_1000'])
    #fig.savefig('visualizations/6_Top_Bottom_Zips_Boxplot.png')
    print("\nVizualization can be found in folder *visualizations*. Filename:6_Top_Bottom_Zips_Boxplot.png")
    print("----------------------------------------------------------------------------------------------")    
    plt.show()
    #############################

    #Plot the most common vehicles models in the Top 5 and Bottom 5 zipcodes
    print("\n\nPlot the most common vehicles models in the Top 5 and Bottom 5 zipcodes")
    veh_top = {}
    top_df = pd.DataFrame()
    for zipcode in AQI_highest.Zipcode:
        top_df = vehicles_DF.loc[vehicles_DF['Zip_Code'] == zipcode][['Zip_Code','Make','Vehicles']]
        x = top_df.groupby(['Make']).sum()['Vehicles'].sort_values(ascending=False)[:6]
        for i in range(6):
            veh_top[x.index[i]] = veh_top.get(x.index[i],0) + x.values[i]
    veh_top.pop('OTHER/UNK', None)
    veh_top.pop('UNK', None)
    veh_top.pop('OTHER', None)
    veh_top = sorted(veh_top.items(), key=lambda item: item[1],reverse=True)[:5]
    df = pd.DataFrame(veh_top,columns=['Model','Top_Zipcodes_No.OfVehicles'])

    veh_bottom ={}
    bottom_df = pd.DataFrame()
    for zipcode in AQI_least.Zipcode:
        bottom_df = vehicles_DF.loc[vehicles_DF['Zip_Code'] == zipcode][['Zip_Code','Make','Vehicles']]
        y = bottom_df.groupby(['Make']).sum()['Vehicles'].sort_values(ascending=False)[:6]
        for i in range(6):
            veh_bottom[x.index[i]] = veh_bottom.get(x.index[i],0) + x.values[i]
    veh_bottom.pop('OTHER/UNK', None)
    veh_bottom.pop('UNK', None)
    veh_bottom.pop('OTHER', None)
    veh_bottom = sorted(veh_bottom.items(), key=lambda item: item[1],reverse=True)[:5]
    df1 = pd.DataFrame(veh_bottom,columns=['Model','Bottom_Zipcodes_No.OfVehicles'])
    df = df.merge(df1, on='Model',how='left')

    fig, ax = plt.subplots()
    df.plot.bar(x = 'Model', y = ['Top_Zipcodes_No.OfVehicles', 'Bottom_Zipcodes_No.OfVehicles'],            rot = 40, ax = ax,figsize=(8,5),color=['#EC6B56','#47B39C'])
    for p in ax.patches: 
        ax.annotate(np.round(p.get_height(),decimals=2), (p.get_x()+p.get_width()/20., p.get_height()))
    ax.set_title('Top 5 Vehicle Models and Distibution in Top and Bottom 5 Zipcodes',              fontdict={'size':10})
    ax.set(ylabel='No. of Vehicles')
    plt.legend(["Zipcodes with Highest AQI","Zipcodes with Lowest AQI"])
    #fig.savefig('visualizations/7_Top_Bottom_Zips_Model.png')
    print("\nVizualization can be found in folder *visualizations*. Filename:7_Top_Bottom_Zips_Model.png")
    print("----------------------------------------------------------------------------------------------")    
    plt.show()
    #############################

    #Percent Distribution of dominant Pollutant
    print("\n\nPercent Distribution of dominant Pollutant using Pie Chart..")
    AQI_sort = dataset.sort_values(by=['Air_Quality_Index'], ascending=True)
    #Best AQI - 5 Zipcodes
    AQI_highest = AQI_sort[-5:]
    #Worst AQI - 5 Zipcodes
    AQI_least = AQI_sort[:5]
    colours = {'o3': '#EC6B56',
               'pm10': '#47B39C',
               'pm25': '#FFC154'}
    fig, (ax1,ax2,ax3) = plt.subplots(1,3,figsize=(10,10))
    pie_val = dataset["Dominant_Pollutant"].value_counts()
    labels=pie_val.index
    ax1.pie(pie_val.values, labels=pie_val.index, labeldistance=1.15,        autopct='%1.1f%%',wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white' },colors=[colours[key] for key in labels]);
    ax1.set_title("Across all Zipcodes")
    pie_val = AQI_highest["Dominant_Pollutant"].value_counts()
    labels=pie_val.index
    ax2.pie(pie_val.values, labels=pie_val.index, labeldistance=1.15,        autopct='%1.1f%%',wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white' },colors=[colours[key] for key in labels]);
    ax2.set_title("Zipcodes with Highest AQI")
    pie_val = AQI_least["Dominant_Pollutant"].value_counts()
    labels=pie_val.index
    ax3.pie(pie_val.values, labels=pie_val.index, labeldistance=1.15,        autopct='%1.1f%%',wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white' },colors=[colours[key] for key in labels]);
    ax3.set_title("Zipcodes with Least AQI")
    plt.suptitle("% Distribution of Dominant Pollutant")
    plt.subplots_adjust(top=1.5)
    #fig.savefig('visualizations/8_Top_Bottom_Zips_Dom_Poll_Pie.png')
    print("\nVizualization can be found in folder *visualizations*. Filename:8_Top_Bottom_Zips_Dom_Poll_Pie.png")
    print("----------------------------------------------------------------------------------------------")    
    plt.show();
    #############################

    #Linear Regression - 1
    print("\n\nLINEAR REGRESSION")
    print("=================")
    df = df_per_1000
    cols = ['Total_Population','Per_Capita_Income','Carbon_Monoxide',
           'Nitrogen_Dioxide', 'Ozone', 'Inhalable_Particulate_Matter_pm10',
           'Fine_Particulate_Matter_pm25', 'Sulfur_Dioxide',
           'Hybrid_Vehicles_Per_1000', 'Electric_Vehicles_Per_1000',
           'Fuel_Vehicles_Per_1000']
    print("\n\nRunning Linear Regression with following factors:\n",cols)

    X = np.column_stack((df['Total_Population'],df['Average_Household_Income'],df['Per_Capita_Income'],df['Carbon_Monoxide'],
    df['Nitrogen_Dioxide'],df['Ozone'],df['Inhalable_Particulate_Matter_pm10'],
    df['Fine_Particulate_Matter_pm25'],df['Sulfur_Dioxide'],
    df['Hybrid_Vehicles_Per_1000'],df['Electric_Vehicles_Per_1000'],
    df['Fuel_Vehicles_Per_1000']))
    y = df['Air_Quality_Index']
    X2 = sm.add_constant(X)
    est = sm.OLS(y, X2)
    est2 = est.fit()
    print("\n",est2.summary())
    print("\n\nSince condition number is large, 1.3e+06,there is chance of high multicollinearity.Executing linear regression with fewer factors.")
    print("#############################\n")
    #############################
    #Linear Regression - 2
    cols = ['Per_Capita_Income','Carbon_Monoxide',
           'Nitrogen_Dioxide', 'Ozone', 'Inhalable_Particulate_Matter_pm10',
           'Fine_Particulate_Matter_pm25', 'Sulfur_Dioxide','Electric_Vehicles_Per_1000']
    print("\n\nRunning Linear Regression with following factors:\n",cols)

    X = np.column_stack((df['Per_Capita_Income'],
    df['Nitrogen_Dioxide'],df['Ozone'],df['Inhalable_Particulate_Matter_pm10'],
    df['Fine_Particulate_Matter_pm25'],df['Sulfur_Dioxide'],df['Hybrid_Vehicles_Per_1000'],df['Electric_Vehicles_Per_1000'],
    df['Fuel_Vehicles_Per_1000']))
    y = df['Air_Quality_Index']
    X2 = sm.add_constant(X)
    est = sm.OLS(y, X2)
    est2 = est.fit()
    print("\n",est2.summary())
    print("\n\nSince condition number is large, 4.24e+05,there is chance of high multicollinearity.Executing linear regression with fewer factors.")
    print("#############################\n")
    #############################
    #Linear Regression - 3

    df = df_per_1000
    cols = ['Nitrogen_Dioxide', 'Ozone', 'Inhalable_Particulate_Matter_pm10',
           'Fine_Particulate_Matter_pm25', 'Sulfur_Dioxide']
    print("\n\nRunning Linear Regression with following factors:\n",cols)
    X = np.column_stack((df['Nitrogen_Dioxide'],df['Ozone'],df['Inhalable_Particulate_Matter_pm10'],
    df['Fine_Particulate_Matter_pm25'],df['Sulfur_Dioxide']))
    y = df['Air_Quality_Index']
    X2 = sm.add_constant(X)
    est = sm.OLS(y, X2)
    est2 = est.fit()
    print(est2.summary())
    
    Xs = df[cols]
    y = df['Air_Quality_Index'].values.reshape(-1,1)
    reg = LinearRegression()
    model = reg.fit(Xs, y)
    disp = "The linear model is: Y = {:.5} "
    for col in cols:
        disp += "+{:.5}*"+col
    #print(disp)
    x=""
    for i in range(len(cols)):
        x+=", reg.coef_[0]["+str(i)+"]"
    #print(x)
    print("\n\n",disp.format(reg.intercept_[0],reg.coef_[0][0],reg.coef_[0][1],reg.coef_[0][2],reg.coef_[0][3],reg.coef_[0][4]))
    res = pd.DataFrame(reg.coef_[0], 
                 cols, 
                 columns=['coef'])\
                .sort_values(by='coef', ascending=False)
    print("\n\nLinear Regression coeffecients:\n",res)

    #############################

    #California counties CHOROPLETH MAP using AQI
    print("CHOROPLETH MAP of California to visulize AQI patterns..")
    #Get County stats
    countyData = dataset.groupby("County_Name").mean()
    df_sample = pd.read_csv('map.csv')
    df_sample_r = df_sample[df_sample['STNAME'] == 'California'][['FIPS', 'STNAME', 'CTYNAME']]
    data = df_sample_r.merge(countyData, left_on='CTYNAME', right_on='County_Name', how='left')

    values = data['Air_Quality_Index'].tolist()
    fips = data['FIPS'].tolist()
    colorscale = ['#ffffcc','#d9f0a3','#addd8e','#78c679','#41ab5d','#238443','#005a32']
    colorscale =['#d3f2a3','#97e196','#6cc08b','#4c9b82','#217a79','#105965','#074050']
    colorscale = ['#f7feae','#b7e6a5','#7ccba2','#46aea0','#089099','#00718b','#045275','#045275']
    fig = ff.create_choropleth(
        fips=fips, values=values, scope=['CA', 'AZ', 'Nevada',' Idaho'],
        binning_endpoints=[45,50,55,60,65,75], colorscale=colorscale,
        county_outline={'color': 'rgb(255,255,255)', 'width': 0.5}, round_legend_values=True,
        legend_title='AQI Range', title='California Air Quality Index',show_hover=True
    )
    fig = map_hover(fig,data)
    fig.layout.template = None           
    fig.show()

    print("\nVizualization can be found in folder *visualizations*. Filename:9_Choropleth_AQI.png")
    print("----------------------------------------------------------------------------------------------")
    #############################
    
    #California counties CHOROPLETH MAP using Ozone concentration
    print("CHOROPLETH MAP of California to visulize Ozone concentration patterns..")
    #Get County stats
    countyData = dataset.groupby("County_Name").mean()
    df_sample = pd.read_csv('map.csv')
    df_sample_r = df_sample[df_sample['STNAME'] == 'California'][['FIPS', 'STNAME', 'CTYNAME']]
    data = df_sample_r.merge(countyData, left_on='CTYNAME', right_on='County_Name', how='left')

    values = data['Ozone'].tolist()
    fips = data['FIPS'].tolist()
    colorscale = ['#ffffcc','#d9f0a3','#addd8e','#78c679','#41ab5d','#238443','#005a32']
    colorscale =['#d3f2a3','#97e196','#6cc08b','#4c9b82','#217a79','#105965','#074050']
    colorscale = ['#f6d2a9','#f5b78e','#f19c7c','#ea8171','#dd686c','#ca5268','#b13f64','#b13f64']
    fig = ff.create_choropleth(
        fips=fips, values=values, scope=['CA', 'AZ', 'Nevada',' Idaho'],
        binning_endpoints=[40,45,50,55,60,65], colorscale=colorscale,
        county_outline={'color': 'rgb(255,255,255)', 'width': 0.5}, round_legend_values=True,
        legend_title='Ozone Range(Unit: ppb)', title='California Ozone Concentration',show_hover=True
    )
    fig = map_hover(fig,data)
    fig.layout.template = None           
    fig.show()
    print("\nVizualization can be found in folder *visualizations*. Filename:10_Choropleth_Ozone.png")
    print("----------------------------------------------------------------------------------------------")
    #############################
    
    #California counties CHOROPLETH MAP using pm10 concentration
    print("CHOROPLETH MAP of California to visulize pm10 concentration patterns..")
    #Get County stats
    countyData = dataset.groupby("County_Name").mean()
    df_sample = pd.read_csv('map.csv')
    df_sample_r = df_sample[df_sample['STNAME'] == 'California'][['FIPS', 'STNAME', 'CTYNAME']]
    data = df_sample_r.merge(countyData, left_on='CTYNAME', right_on='County_Name', how='left')

    values = data['Inhalable_Particulate_Matter_pm10'].tolist()
    fips = data['FIPS'].tolist()
    colorscale = ['#ffffcc','#d9f0a3','#addd8e','#78c679','#41ab5d','#238443','#005a32']
    colorscale =['#d3f2a3','#97e196','#6cc08b','#4c9b82','#217a79','#105965','#074050']
    colorscale = ['#f3e79b','#fac484','#f8a07e','#eb7f86','#ce6693','#a059a0','#5c53a5']
    fig = ff.create_choropleth(
        fips=fips, values=values, scope=['CA', 'AZ', 'Nevada',' Idaho'],
        binning_endpoints=[35,40,45,55,65], colorscale=colorscale,
        county_outline={'color': 'rgb(255,255,255)', 'width': 0.5}, round_legend_values=True,
        legend_title='pm10 Range(Unit: ug/m3)', title='California pm10 Concentration',show_hover=True
    )
    fig = map_hover(fig,data)
    fig.layout.template = None           
    fig.show()
    print("\nVizualization can be found in folder *visualizations*. Filename:11_Choropleth_pm10.png")
    print("----------------------------------------------------------------------------------------------")    

################################################################################################################

# Main program STARTS HERE!     
#Get user input
parser = argparse.ArgumentParser()
parser.add_argument("--static",action='store_true',help="To view stats and sample data of static tables in Database.")
args = parser.parse_args()

if args.static:
    #Print 5 rows of each dataset
    print("Option Chosen: --static")
    print("The program will print stats,sample data from each static dataset and run analysis.")
    get_static_data("Dataset/")
    dataset = merge_data()
    db_name = "Dataset/Project_DataBase_California.db"
    vehicles_DF = get_database_DF(db_name,\
                              "SELECT * FROM California_Zipcodes_Vehicles ORDER BY Zip_Code")
    
else:
    #Scrape whole datasets
    print("Option Chosen:Scrape whole")
    print("The program will scrape whole datasets,print 5 sample rows and run analysis on the scraped data.")
    scrape_whole()    
    dataset = merge_data_scrape()
    db_name = "Dataset/Project_DataBase_California_Scraped.db"
    vehicles_DF = get_database_DF(db_name,\
                              "SELECT * FROM California_Zipcodes_Vehicles ORDER BY Zip_Code")
    
#Change values of number of vehicles to per 1000people 
df_per_1000 = clean_data(dataset)
get_genrl_stats()
#Analysis
analysis(dataset,vehicles_DF)
