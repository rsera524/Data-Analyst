#Load the pandas and sqlite3 libraries and establish a connection to FinalDB.db

import csv, sqlite3
con = sqlite3.connect("FinalDB.db")
cur = con.cursor()
!pip install -q pandas==1.1.5

#Load the SQL magic module

%load_ext sql



#Establish a connection between SQL magic module and the database FinalDB.db
%sql sqlite:///FinalDB.db

#Use Pandas to load the data available in the links above to dataframes. 
#Use these dataframes to load data on to the database FinalDB.db as required tables.

import pandas
df= pandas.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoCensusData.csv")
df.to_sql("Chicago_Census_Data",con, if_exists = 'replace', index = False, method = "multi")

df= pandas.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoPublicSchools.csv")
df.to_sql("Chicago_Public_Schools",con, if_exists = 'replace', index = False, method = "multi")

df= pandas.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoCrimeData.csv")
df.to_sql("Chicago_Crime_Data",con, if_exists = 'replace', index = False, method = "multi")

#Find the total number of crimes recorded in the CRIME table.

%sql Select count(ID) from Chicago_Crime_Data

#List community area names and numbers with per capita income less than 11000.

%sql Select COMMUNITY_AREA_NAME, COMMUNITY_AREA_NUMBER, PER_CAPITA_INCOME \
from Chicago_Census_Data where PER_CAPITA_INCOME < 11000


#List all case numbers for crimes involving minors?
#(children are not considered minors for the purposes of crime analysis)

%sql Select CASE_NUMBER from Chicago_Crime_Data where DESCRIPTION Like '%minor%'

#List all kidnapping crimes involving a child?

%sql Select CASE_NUMBER from Chicago_Crime_Data where DESCRIPTION Like '%child%' \
and PRIMARY_TYPE = "KIDNAPPING"


#List the kind of crimes that were recorded at schools. (No repetitions)

%sql Select Distinct(PRIMARY_TYPE) from Chicago_Crime_Data where LOCATION_DESCRIPTION Like '%school%'


 #List the type of schools along with the average safety score for each type.

%sql Select "Elementary, Middle, or High School", avg(SAFETY_SCORE) \
AS AVERAGE From Chicago_Public_Schools \
Group by "Elementary, Middle, or High School"




#List 5 community areas with highest % of households below poverty line.

%sql Select COMMUNITY_AREA_NAME, PERCENT_HOUSEHOLDS_BELOW_POVERTY \
FROM Chicago_Census_Data \
ORDER BY PERCENT_HOUSEHOLDS_BELOW_POVERTY DESC Limit 5



#Which community area is most crime prone? Display the coumminty area number only.

%sql Select COMMUNITY_AREA_NUMBER from \
Chicago_Crime_Data group by COMMUNITY_AREA_NUMBER order by Count(ID) Desc Limit 1



#Use a sub-query to find the name of the community area with highest hardship index

%sql Select COMMUNITY_AREA_NAME from Chicago_Census_Data\
where HARDSHIP_INDEX = (Select max(HARDSHIP_INDEX) from Chicago_Census_Data)


#Use a sub-query to determine the Community Area Name with most number of crimes?

%sql Select COMMUNITY_AREA_NAME \
From Chicago_Census_Data \
where COMMUNITY_AREA_NUMBER in(Select COMMUNITY_AREA_NUMBER from Chicago_Crime_Data \
Group by COMMUNITY_AREA_NUMBER Order by Count(ID) Desc Limit 1)




