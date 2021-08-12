# DSCI510 - Project

## Brief Description of Project
Analyse the relationship between the air quality index and the number of vehicles of each
fuel type in California state (grouped by zip codes)

## Files and Folder Structure

The main folder shruti-krishna-kumar-DSCI510-project.zip contains the following files
- run_project.py
Runs the program to extract all datasets and run analysis.
- scraper.py
Runs the program to extract all datasets.
- Scrape_California_Zipcodes.py
Contains code to extract all California zipcodes.
- Scrape_California_Geocodes.py
Contains code to extract all California geocodes.
- Scrape_California_AQI.py
Contains code to extract all California Air Quality Index
- Scrape_California_Vehicles.py
Contains code to extract all California vehicle type information
- requirements.txt
The python packages required to run the program
- shruti-krishna-kumar-DSCI510-project-report.pdf
Report describing project and analysis done.
- readme.txt
Details about project and execution of scripts and inputs.
- readme.md
Details about project and execution of scripts and inputs.

Folder 'Dataset' has the database file of the project
- Project_DataBase_California.db
Contains all data scraped for the project in 4 tables.

## Database Details
Database file: Project_DataBase_California.db (inside folder Dataset)
There are 4 tables in the database: 
1. California_Zipcodes
Stores all Zipcodes of California scraped from
https://california.hometownlocator.com/zip-codes/counties.cfm
Number of rows: 2547
2. California_Zipcodes_Vehicles
Stores Vehicles data of all Zipcodes of California from API https://data.ca.gov
Number of rows: 602394
3. California_Zipcodes_Geocodes
Stores Geocodes of all Zipcodes of California from API http://www.mapquestapi.com
Number of rows: 2547
4. California_Zipcodes_AQI
Stores AQI data of all Zipcodes of California from API https://api.breezometer.com/air-quality
Number of rows: 2547

## How to run the project
Please ensure python version >=3.7.1
In the python terminal please run the below command
Please create a new environment and run these 4 commands
$ pip install -r requirements.txt
$conda install -c plotly plotly-geo
$conda install -c https://conda.anaconda.org/plotly plotly
$conda install -c conda-forge geopandas


In the command prompt the following commands can be executed
1.run_project.py --static
The program will print stats and 5 rows from each static dataset and run analysis on the whole dataset after merging the data.
The static dataset is in the file 'Project_DataBase_California.db' in the folder 'Dataset'.
Time: < 2 minutes
2. run_project.py
The program will scrape whole datasets and print 5 sample rows and run analysis on the whole dataset after merging the data.
This will generate .db file named 'Project_DataBase_California_Scraped.db' in the folder 'Dataset'.
Time: approximately 4.5 hours.

## Sample inputs and outputs:
The project does not require any input from the user. 
Sample Output

There are 4 tables being used in this project:

California_Zipcodes
California_Zipcodes_AQI
California_Zipcodes_Vehicles
California_Zipcodes_Geocodes


Table  1 :  California_Zipcodes


Table  1 California_Zipcodes  Stats
Number of rows in table:  2587

Column ID  |           Column Name           |          Column Data Type
0           Zipcode                                         text
1           Zipcode_URL                                     text

...
Table  1 California_Zipcodes  :Sample Data
Table Columns Names:
 ['Zipcode', 'Zipcode_URL', 'County_Name', 'Total_Population', 'Population_in_Households', 'Population_in_Families', 'Population_in_Group_Quarters1', 'Population_Density', 'Diversity_Index2', 'Median_Household_Income', 'Average_Household_Income', 'Percentage_of_Income_for_Mortgage4', 'Per_Capita_Income', 'Wealth_Index5', 'Total_Housing_Units', 'Owner_Occupied_HU', 'Renter_Occupied_HU', 'Vacant_Housing_Units', 'Median_Home_Value', 'Average_Home_Value', 'Housing_Affordability_Index3', 'Total_Households', 'Average_Household_Size', 'Family_Households', 'Average_Family_Size']

5 rows from Table:
('94501', 'https://california.hometownlocator.com/zip-codes/data,zipcode,94501.cfm', 'Alameda', 63137.0, 61691.0, 46466.0, 1446.0, 7828.0, 73.0, 103090.0, 139509.0, 37.0, 57567.0, 146.0, 27731.0, 10801.0, 15110.0, 1820.0, 912803.0, 986432.0, 64.0, 25911.0, 2.38, 15022.0, 3.0)
('94502', 'https://california.hometownlocator.com/zip-codes/data,zipcode,94502.cfm', 'Alameda', 13972.0, 13960.0, 12046.0, 12.0, 5267.0, 64.0, 155327.0, 196339.0, 27.0, 72525.0, 314.0, 5292.0, 4255.0, 906.0, 131.0, 1001874.0, 1072885.0, 86.0, 5161.0, 2.7, 3853.0, 3.0)
..

Air Quality Index Category Distribution using Bar Plot..

Vizualization can be found in folder *visualizations*. Filename:1_AQI_Category_Distribution.png
----------------------------------------------------------------------------------------------


Bar Chart of Air Quality Index by County..

Vizualization can be found in folder *visualizations*. Filename:2_AQI_County_Distribution.png
----------------------------------------------------------------------------------------------


Correlation Matrix of Numerical Fatures..

Vizualization can be found in folder *visualizations*. Filename:3_Corr_Matrix.png
----------------------------------------------------------------------------------------------


Regression plot for Per_Capita_Income and Total_Population against Air_Quality_Index..

Vizualization can be found in folder *visualizations*. Filename:4_Regplot_Pop_Income.png
----------------------------------------------------------------------------------------------


Regression plot for change in AQI wrt Vehicles fuel type..

Vizualization can be found in folder *visualizations*. Filename:5_Regplot_Vehicles.png
----------------------------------------------------------------------------------------------


Compare vehicles of Zipcodes with highest and lowest AQI and visualize using Boxplot

Vizualization can be found in folder *visualizations*. Filename:6_Top_Bottom_Zips_Boxplot.png
----------------------------------------------------------------------------------------------


Plot the most common vehicles models in the Top 5 and Bottom 5 zipcodes

Vizualization can be found in folder *visualizations*. Filename:7_Top_Bottom_Zips_Model.png
----------------------------------------------------------------------------------------------


Percent Distribution of dominant Pollutant using Pie Chart..

Vizualization can be found in folder *visualizations*. Filename:8_Top_Bottom_Zips_Dom_Poll_Pie.png
----------------------------------------------------------------------------------------------


LINEAR REGRESSION
=================
Running Linear Regression with following factors:
 ['Total_Population', 'Per_Capita_Income', 'Carbon_Monoxide', 'Nitrogen_Dioxide', 'Ozone', 'Inhalable_Particulate_Matter_pm10', 'Fine_Particulate_Matter_pm25', 'Sulfur_Dioxide', 'Hybrid_Vehicles_Per_1000', 'Electric_Vehicles_Per_1000', 'Fuel_Vehicles_Per_1000']

                             OLS Regression Results
==============================================================================
Dep. Variable:      Air_Quality_Index   R-squared:                       0.810
Model:                            OLS   Adj. R-squared:                  0.808

CHOROPLETH MAP of California to visulize AQI patterns..

Vizualization can be found in folder *visualizations*. Filename:9_Choropleth_AQI.png
----------------------------------------------------------------------------------------------
CHOROPLETH MAP of California to visulize Ozone concentration patterns..

Vizualization can be found in folder *visualizations*. Filename:10_Choropleth_Ozone.png
----------------------------------------------------------------------------------------------
CHOROPLETH MAP of California to visulize pm10 concentration patterns..

Vizualization can be found in folder *visualizations*. Filename:11_Choropleth_pm10.png
----------------------------------------------------------------------------------------------



