from cgitb import text
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


def executaCarga():

    #INSTALL CRYPTOGRAPHY and all of the above

    warnings.filterwarnings('ignore')

    config = configparser.ConfigParser()
    config.read('config.ini')

    def ora_database_connect2():
        user = config['ORACLE2']['user']
        password = config['ORACLE2']['password']
        ip = config['ORACLE2']['ip']
        sid = config['ORACLE2']['sid']
        #cx_Oracle.init_oracle_client(lib_dir=config['ORACLE2']['oracle_client_config_file']) 
        #ATIVA A LINHA ACIMA QUANDO FOR FAZER TESTE SOMENTE COM O RH
        
        connection = cx_Oracle.connect(
            user,
            password,
            f'{ip}/{sid}',
            encoding="UTF-8"
            )

        return connection

    def mysql_database_connect():
        username = config['MYSQL']['username']
        password = config['MYSQL']['password']
        port = config['MYSQL']['port']
        hostname = config['MYSQL']['hostname']
        schema_name = config['MYSQL']['schema_name']

        #print("mysql+pymysql://"+username+":"+password+"@"+hostname+":"+port+"/"+schema_name)
        engine = create_engine("mysql+pymysql://"+username+":"+password+"@"+hostname+":"+port+"/"+schema_name)

        return engine

    def load_Cargas(connect_ora, mysql_engine,origem ):
        print('Loading Cargas Recursos Humanso....')
        print(datetime.datetime.today())
    

        #print(data_atual)
        #busca todas as tabelas que dos modulos do BI
        sql = "SELECT id, tabela,tipo,modulo,sequencia,string_del,string_ins FROM dw_bifna.queries  WHERE db_origem = '"+origem+"'  order by tipo asc,sequencia asc;"
        data=pd.read_sql(sql,mysql_engine)
        
        for i in range(len(data)):
            print(data.tabela[i])
            
            #Executa a exclusão
            mysql_engine.execute(data.string_del[i])
            
            #exectua a inclusao
            data2=pd.read_sql(str(data.string_ins[i]),connect_ora)
            #print("Total records form Oracle : ", data2.shape[0])
            data2.to_sql(data.tabela[i], con=mysql_engine, if_exists='append', index=False, chunksize=10000)
            
            #executa o update na tabela para infrormar a data de atualização da mesma
            data_atual = datetime.datetime.today() 
            sqlUp = "update queries set dt_carga = '"+ str(data_atual) + "' where id = " + str(data.id[i])
            mysql_engine.execute(sqlUp)


    #while True:
    print('Executando sincronismo ERP x BI...')
    print(datetime.datetime.now())

    #Connectar bases  ERP
    c_ora2 = ora_database_connect2() #ERP 
    mysql_e = mysql_database_connect()
    
    #loading tables
    load_Cargas(c_ora2,mysql_e,'ORACLE2')


    #finalizar  connection
    c_ora2.close()

    mysql_e.dispose()
    print("Encerrado o sincronismo...")
    print(datetime.datetime.now())