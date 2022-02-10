from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from dateutil.relativedelta import relativedelta
from datetime import datetime
from dateutil import rrule
import asyncio
import aiohttp

args = {
    'owner': 'administrator',
    'start_date': days_ago(1)
}


async def get_latest_info(ti):
    rate_info = list()
    url = 'https://api.exchangerate.host/latest'
    async with aiohttp.ClientSession() as session:
        async with session.get(url, json={'base': 'USD'}) as response:
            data = await response.json()
            rate_data = dict()
            rate_data['rate'] = format(data['rates']['BTC'], '.6f')
            rate_data['date'] = str(datetime.date(datetime.now()))
            rate_data['is_latest'] = True
            rate_info.append(rate_data)
    ti.xcom_push(key='rate_info', value=rate_info)


async def historical_info(dates: list, session: aiohttp.client.ClientSession, ti):
    rate_info = list()
    for date in dates:
        url = f'https://api.exchangerate.host/{date}'
        async with session.get(url, json={'base': 'USD'}) as response:
            data = await response.json()
            rate_data = dict()
            rate_data['rate'] = format(data['rates']['BTC'], '.6f')
            rate_data['date'] = date
            rate_data['is_latest'] = False
            rate_info.append(rate_data)
    ti.xcom_push(key='rate_info', value=rate_info)


async def get_historical_info(ti):
    dates = list()
    today = datetime.now()
    worker = 5
    tasks = list()
    for dt in rrule.rrule(rrule.DAILY, dtstart=today - relativedelta(years=1), until=today - relativedelta(days=1)):
        dates.append(str(datetime.date(dt)))
    async with aiohttp.ClientSession() as session:
        for i in range(worker):
            task = asyncio.create_task(historical_info(dates[i::worker], session, ti))
            tasks.append(task)
        await asyncio.gather(*tasks)


def run_parse_historical_info(ti):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_historical_info(ti))


def run_parse_latest_info(ti):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_latest_info(ti))


def insert_or_update(ti):
    rate_informations = ti.xcom_pull(key='rate_info', task_ids=['parse_historical_info', 'parse_latest_info'])
    pg_hook = PostgresHook(postgres_conn_id='airflow', schema='airflow')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    for i in range(len(rate_informations)):
        for j in range(len(rate_informations[i])):
            query = f'''INSERT INTO exchangerate.btc_to_dollar (date,rate,is_latest)
                            VALUES('{rate_informations[i][j]['date']}','{rate_informations[i][j]['rate']}',
                            '{rate_informations[i][j]['is_latest']}')
                            ON CONFLICT (date)
                            DO
                               UPDATE SET rate = EXCLUDED.rate,is_latest = EXCLUDED.is_latest;'''
            cursor.execute(query)
    connection.commit()


with DAG(dag_id='parse_rate_informations', default_args=args, schedule_interval='0 */3 * * *', catchup=False) as dag:
    postgre_task_1 = PostgresOperator(
        task_id='create_scheme_if_not_exsists',
        postgres_conn_id='airflow',
        sql='CREATE SCHEMA IF NOT EXISTS exchangerate;',
        do_xcom_push=False
    )

    postgre_task_2 = PostgresOperator(
        task_id='create_table_if_not_exsists',
        postgres_conn_id='airflow',
        sql='''CREATE TABLE IF NOT EXISTS exchangerate.btc_to_dollar (
                    date date NOT NULL PRIMARY KEY,
                    rate numeric(7,6),
                    is_latest bool)''',
        do_xcom_push=False
    )

    python_task_1 = PythonOperator(
        task_id='parse_historical_info',
        python_callable=run_parse_historical_info,
        provide_context=True
    )

    python_task_2 = PythonOperator(
        task_id='parse_latest_info',
        python_callable=run_parse_latest_info,
        provide_context=True
    )

    python_task_3 = PythonOperator(
        task_id='write_to_db',
        python_callable=insert_or_update
    )

    postgre_task_1 >> postgre_task_2 >> python_task_1 >> python_task_3
    postgre_task_1 >> postgre_task_2 >> python_task_2 >> python_task_3
