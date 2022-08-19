# 6/25/2022 Example of a simple script to load CSVs in a folder. Because the columns were dynamic (ie, some columns can be omitted depending on the data), I needed to hard code the columns to avoid exceptions.



# importing the required modules
from cmath import nan
import glob
from tarfile import RECORDSIZE
import pandas as pd
import pyodbc 
from sqlalchemy import create_engine, event
import os 
import logging
import shutil
import copy
 



# mssql+pyodbc://<username>:<password>@<dsnname>

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=XXXXX;'
                      'Database=PP_TST;'
                      'Trusted_Connection=yes;')
engine = create_engine("mssql+pyodbc://@TST_DB", echo=True, hide_parameters=True)
cursor = conn.cursor()  

#set path for folder containing CSVs
path = 'I:/Daily Export/Company/Daily Summary' 

# defining an empty list to store content
data_frame = pd.DataFrame()
content = []

# checking all the csv files in the specified path
os.chdir(path)
path =  os.getcwd() 
files = glob.glob(path + "/*.csv") 
    #files = os.path.join(path, "/*.csv")
if len(files) > 0:
    for fp in files:
            # reading content of csv file, create new columns based on filename (for creation date, company name, etc)
        try:
            df = pd.concat((pd.read_csv(fp, skiprows=0, thousands=',' ,).assign(file_name=os.path.basename(fp).split('.')[0])
                for fp in files))   
            content.append(df)
        except UnicodeDecodeError:
            df = pd.concat((pd.read_csv(fp, skiprows=0, thousands=',' ).assign(file_name=os.path.basename(fp).split('.')[0])
                for fp in files)) 
            content.append(fp)


            

####move csv files to archive folder            
path =  os.getcwd() #"csvfoldergfg"
files = glob.glob(path + "/*.csv") 
if len(files) > 0:
    for fp in files:
        shutil.move(fp, 'I:/Daily Export/Company/Daily Summary/Archive') 
        print(fp, " moved to Daily Summary Archive folder.")



#update setting to display all columns for print and head
pd.set_option('display.max_columns', None) 

### Transformations #### 
df[['Job_Portal', 'DailySummary', 'SumDate']] = df['file_name'].str.split('_', expand=True)
df['Date'] = pd.to_datetime(df['Date'])
df['SumDate'] = pd.to_datetime(df['SumDate'])
df['CreatedOn'] = pd.to_datetime(pd.Timestamp.today().strftime('%Y-%m-%d, %r'))
df['CreatedBy'] = 'autogen'
# df[["Cost"]] = df[["Cost"]].replace('[\$,\%]', '', regex=True).apply(pd.to_numeric)
df[["Clicks", "Cost"]] = df[["Clicks", "Cost"]].apply(pd.to_numeric)
df.columns = df.columns.str.replace(' ', '_')


#print(df)
newdf = df[['Date', 'Organization', 'Status', 'Clicks', 'Cost', 'Candidates', 'CreatedOn', 'CreatedBy', 'Job_Portal', 'SumDate']].copy()

## group records to remove duplicates ##
groupeddf = newdf[newdf.groupby(['Date'])['SumDate'].transform('max') == newdf['SumDate']] 
print(groupeddf)


### connect to sql server and append dataframe to table #### 
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

##### insert dataframe into UpwardDailySummary_TST
with engine.connect() as conn, conn.begin():
    groupeddf.to_sql("CompanyName_TST", con=engine, if_exists="append", index=False, schema='dbo')
    cursor.execute('SELECT * FROM CompanyNamedDailySummary_TST')

cursor.commit()
cursor.close()



# Create new table based on columns in CSV

# SQL_CREATE_TBL = 'CREATE TABLE AppcastJobs2_TST('
# for name in range(0, num_cols):
#     SQL_CREATE_TBL += '[{}] nvarchar(max), '.format(df2.columns[name])
# SQL_CREATE_TBL = SQL_CREATE_TBL.rstrip(' ,')
# SQL_CREATE_TBL += ');'
 
