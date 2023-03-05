from pathlib import Path
import os
import cx_Oracle
import configparser
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import warnings
import datetime
#import schedule
import time


#INSTALL CRYPTOGRAPHY and all of the above

warnings.filterwarnings('ignore')

config = configparser.ConfigParser()
config.read('config.ini')
 
def mysql_database_connect():
    username = config['MYSQL']['username']
    password = config['MYSQL']['password']
    port = config['MYSQL']['port']
    hostname = config['MYSQL']['hostname']
    schema_name = config['MYSQL']['schema_name']

    #print("mysql+pymysql://"+username+":"+password+"@"+hostname+":"+port+"/"+schema_name)
    engine = create_engine("mysql+pymysql://"+username+":"+password+"@"+hostname+":"+port+"/"+schema_name)

    return engine

def load_tabelas( mysql_engine ):
    print('Loading Dim....')
    print(datetime.datetime.now())
    queries_folder = config['SQL_QUERIES']['tabelas_']
    for file in Path(queries_folder).glob('*.sql'):
        table_name = file.name.split('.')[0]
        print(table_name)

        #buidling sql statement to select records from Oracle
        sql = file.read_text().split(";")[0]
        sql= "insert into  queries values(null,'"+table_name+"','"+sql+"',null,'D',null,null,0)"
        print(sql)
        
        # insert into mysql
        mysql_engine.execute(sql)
 
        print("Data pushed success")
    print(datetime.datetime.now())
 



print('Executando sincronismo Focco x BI...')
print(datetime.datetime.now())
#if datetime.datetime.now().hour == 23:

#Create sqlalchemy engine
mysql_e = mysql_database_connect()

#loading tables
load_tabelas(mysql_e)
 
#close connection
mysql_e.dispose()
print("Encerrado o sincronismo...")
 

