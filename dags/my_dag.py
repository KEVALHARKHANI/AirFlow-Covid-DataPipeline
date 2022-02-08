from airflow import DAG
from sqlalchemy import create_engine
from datetime import datetime
from airflow.operators.python import PythonOperator
from custome_fun import covert_numeric,reformat_data
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import configparser


dag_path = os.getcwd()
def first(ti):
    name = ti.xcom_pull(task_ids='second',key='name')
    print("hello",name)
    return "hello"

def extraction(ti):
    e_flag=False
    raise ValueError("this is test")
    url = 'https://www.worldometers.info/coronavirus/'
    print('url:',url)
    page_source = requests.get(url).content
    data = BeautifulSoup(page_source, 'html.parser')
    all_data = data.find_all('table')
    today_data = all_data[0]
    yeaserday_data = all_data[1]
    twodaysago_data = all_data[2]

    # retriving columns from raw column
    column_data = today_data.find_all('th')
    columns = [i.text for i in column_data]
    columns[0] = 'index'
    for i in range(len(columns)):
        columns[i] = columns[i].replace('\xa0', ' ')
        columns[i] = columns[i].replace('\n', ' ')
    print(columns)

    # cleaning and reformation Covid_data from raw Covid_data
    row_data = []
    tr_data = today_data.find_all('tr', attrs={'data-continent': None})
    for i in tr_data:
        td = []
        td_data = i.find_all('td')
        td = [j.text for j in td_data]
        row_data.append(td)
    df = pd.DataFrame(data=row_data, columns=columns)
    print(df.head())
    df.to_csv(f'{dag_path}/Covid_data/raw_data.csv',index=False)
    e_flag=True
    ti.xcom_push(key='exe_flage',value = e_flag)



def data_transformation(ti):
    df = pd.read_csv(f'{dag_path}/Covid_data/raw_data.csv',dtype=str,)
    df.drop(df.columns[0],axis=1,inplace=True)
    df.drop([0], axis=0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = df.applymap(reformat_data)
    df[['NewCases', 'NewDeaths', 'NewRecovered']] = df[['NewCases', 'NewDeaths', 'NewRecovered']].applymap(
        lambda x: str(x).replace('+', ''))
    df[df.columns[1:14].append(df.columns[15:])] = df[df.columns[1:14].append(df.columns[15:])].applymap(
        lambda x: str(x).replace(',', ''))
    df[df.columns[1:14].append(df.columns[15:])] = df[df.columns[1:14].append(df.columns[15:])].applymap(covert_numeric)
    df[df.columns[1:14].append(df.columns[15:])] = df[df.columns[1:14].append(df.columns[15:])].astype('Int64')
    df.dropna(how=any, thresh=16)
    df.to_csv(f'{dag_path}/Covid_data/Cleaned_data/{str(datetime.today().date())}_Covid.csv')
    ti.xcom_pull(key='exe_flag', value=True)



def data_load(ti):
    config = configparser.ConfigParser()
    config.read(f'{dag_path}/plugins/support_files/db.ini')

    username = config['mysql']['username']
    password = config['mysql']['password']
    database = config['mysql']['database']

    engine = create_engine(f"mysql+pymysql://{username}:{password}@host.docker.internal/{database}")
    conn = engine.connect()
    try:
        df = pd.read_csv(f'{dag_path}/Covid_data/Cleaned_data/{str(datetime.today().date())}_Covid.csv')
        new_col = []

        for i in df.columns:
            i = i.replace(" ", "_")
            i = i.split(",")[0]
            i = i.replace("/", "_per_")
            new_col.append(i)

        df.columns = new_col
        #
        while True:
            exist = conn.execute(f'SELECT id FROM Covid_dates WHERE covid_date = current_date and created = {True}')
            ids = [i for i in exist]
            if not ids:
                conn.execute(f'insert into Covid_dates(created) Values({True})')
            else:
                break

        id = ids[0]
        df_len = df.shape[0]
        print(id[0])
        df['id'] = pd.Series([id[0] for i in range(df_len)])
        print(df.head())
        print(df.dtypes)
        df.to_sql(name="Covid_table", con=conn, if_exists='append', index=False)
    except Exception as E:
        raise Exception(E)
    finally:
        conn.close()






def second(ti):
    ti.xcom_push(key='name',value = 'jerry')
    print("second_task")



with DAG(
    dag_id = "web_scrapt",
    schedule_interval="@daily",
    start_date = datetime(2021,11,1),
    default_args={
        "owner" : "airflow",
        "retries":1,
    },
    catchup=False

) as dag:
    extraction = PythonOperator(
        task_id="extraction",
        python_callable=extraction
    )

    data_transformation = PythonOperator(
        task_id="data_transformation",
        python_callable=data_transformation
    )

    data_load = PythonOperator(
        task_id = "data_load",
        python_callable=data_load
    )

extraction >> data_transformation
data_transformation >> data_load





