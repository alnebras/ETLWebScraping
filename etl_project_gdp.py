from bs4 import BeautifulSoup   
import requests  
import pandas as pd  
import numpy as np  
import sqlite3  
from datetime import datetime  
import pyodbc  

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

server = 'serverName'
database = 'databaseName'
username = 'userName'
password = 'Password'

conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

''' intiailize SQL server connection '''
sql_server_connection = pyodbc.connect(conn_str)

'''Extracts data from the provided URL and returns a DataFrame.'''
def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0], "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

'''Converts GDP values from currency format to float and transforms them from USD (Millions) to USD (Billions).'''
def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x / 1000, 2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return df

'''Saves the DataFrame to a CSV file.'''
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

'''Saves the DataFrame to a SQLite database table.'''
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

'''Executes the specified SQL query on the provided database connection and prints the output.'''
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
 
'''Logs the provided message along with a timestamp to a log file.'''
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

'''Insert the data to SQL server'''
def load_data(connection, df, table_name):
    cursor = connection.cursor()
    for index, row in df.iterrows():
        cursor.execute(f"INSERT INTO {table_name} (Country, GDP_USD_billions) VALUES (?, ?)",
                       row['Country'], row['GDP_USD_billions'])
    connection.commit()
    cursor.close()

load_data(sql_server_connection, df, table_name)
sql_server_connection.close()

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')
