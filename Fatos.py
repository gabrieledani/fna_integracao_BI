import cx_Oracle
import configparser
import pandas as pd
from sqlalchemy import create_engine
import datetime

config = configparser.ConfigParser()
config.read('config.ini')


def executaCarga():
    #warnings.filterwarnings('ignore')

    def ora_database_connect(origem):
        user = config[origem]['user']
        pass_db = config[origem]['password']
        ip = config[origem]['ip']
        port = config[origem]['port']
        sid_db = config[origem]['sid']

        cx_Oracle.init_oracle_client(lib_dir=config[origem]['oracle_client_config_file'])

        dsn_con = cx_Oracle.make
        #engine = create_engine("oracle+cx_oracle://scott:tiger@hostname:1521/dbname?encoding=UTF-8&nencoding=UTF-8")
        #engine = create_engine("oracle+cx_oracle://"+user+":"+pass_db+"@"+ip+":"+port+"/?"+sid_db+"&encoding=UTF-8&nencoding=UTF-8" )

        #return engine
        connection = cx_Oracle.connect(
            user,
            pass_db,
            f'{ip}/{sid_db}',
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
        print('Loading Cargas Fatos....')
        print(datetime.datetime.today())
        #AND TIPO <> 'D'

        #print(data_atual)
        #busca todas as tabelas que dos modulos do BI
        sql = "SELECT id,tabela,tipo,modulo,sequencia,string_del,string_ins FROM dw_bifna.queries WHERE db_origem = '"+origem+"' AND TIPO <> 'D' AND ID = 64 order by tipo asc,sequencia asc;"
        data=pd.read_sql(sql,mysql_engine)
        
        for i in range(len(data)):
            print(data.tabela[i])
            
            #Executa a exclusão
            mysql_engine.execute(data.string_del[i])
            
            #exectua a inclusao
            data2=pd.read_sql(str(data.string_ins[i]),connect_ora)
            print("Total records form Oracle : ", data2.shape[0])

            print(datetime.datetime.today())
            #data2.to_sql(data.tabela[i], con=mysql_engine, if_exists='append', index=False, chunksize=10000)
            
            #executa o update na tabela para infrormar a data de atualização da mesma
            data_atual = datetime.datetime.today() 
            sqlUp = "update queries set dt_carga = '"+ str(data_atual) + "' where id = " + str(data.id[i])
            mysql_engine.execute(sqlUp)


    #while True:
    print('Executando sincronismo ERP x BI...')
    print(datetime.datetime.now())

    #Connectar bases  ERP
    c_ora = ora_database_connect('ORACLE') #ERP 
    mysql_e = mysql_database_connect()
    
    #loading tables
    load_Cargas(c_ora,mysql_e,'ORACLE')

    #finalizar  connection
    c_ora.close()
    mysql_e.dispose()
    print("Encerrado o sincronismo...")
    print(datetime.datetime.now())
