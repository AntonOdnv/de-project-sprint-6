import boto3
import logging
import vertica_python

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
endpoint_url=Variable.get("S3_ENDPOINT_LESSONS6")

session = boto3.session.Session()

s3_client = session.client(
    service_name = 's3',
    endpoint_url = endpoint_url,
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
)

class get_data_from_s3:

    def __init__(self, s3_client):
        self.s3_client = s3_client

    def get_data(self, bucket, key, filename):

        self.s3_client.download_file(
            Bucket = bucket,
            Key = key,
            Filename = filename
        )


@dag(
    schedule_interval=None,  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. 
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint6', 'groups', 'csv', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)

def sprint6_get_groups_csv():

    # Создаем подключение к Vertica.
    vertica_connection = BaseHook.get_connection("vertica_connection")

    autocommit = "True"
    if "autocommit" in vertica_connection.extra_dejson:
        autocommit = vertica_connection.extra_dejson["autocommit"]

    conn_str = {'host': f'{vertica_connection.host}', 
                'port': f'{vertica_connection.port}',
                'user': f'{vertica_connection.login}',       
                'password': f'{vertica_connection.password}',
                'database': f'{vertica_connection.schema}',
                'autocommit': f'{autocommit}'
               }
    
    def fill_table(conn_str, table_name, create_table_query, copy_query):
        with vertica_python.connect(**conn_str) as conn:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS STV2024012236__STAGING.{table_name};")
            cur.execute(create_table_query)
            cur.execute(copy_query)
    
    try:
        s3 = get_data_from_s3(s3_client)
    except Exception as e:
        log.error(f"An error occurred: {str(e)}")
        raise 

    # Генерируем task'и на скачивание файлов из S3
    requests = [
        ('groups.csv', '/data/groups.csv'),
        ('users.csv', '/data/users.csv'),
        ('dialogs.csv', '/data/dialogs.csv'),
        ('group_log.csv', '/data/group_log.csv')
    ]
    tasks = []

    for file_name, file_path in requests:
        @task(task_id=f"get_{file_name.split('.')[0]}")
        def get_s3(file_name, file_path):
            s3.get_data('sprint6', file_name, file_path)
        tasks.append(get_s3(file_name, file_path))

    groups, users, dialogs, group_logs = tasks

    # Перекладываем данные в Vertica. Данные обновляются каждый раз полностью без инкремента.
    @task()
    def fill_groups(conn_str):
        create_table_query = """
                    CREATE TABLE STV2024012236__STAGING.groups
                    (
                        id int NOT NULL PRIMARY KEY,
                        admin_id int,
                        group_name varchar(100),
                        registration_dt timestamp(6),
                        is_private boolean
                    )
                    order by id, admin_id
                    segmented by hash(id) all nodes
                    PARTITION BY registration_dt::date
                    GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);
                    """
        copy_query = """
                    COPY STV2024012236__STAGING.groups(id, admin_id, group_name, registration_dt, is_private)
                    FROM LOCAL '/data/groups.csv'
                    DELIMITER ','
                    ENCLOSED BY '"'
                    REJECTED DATA AS TABLE groups_rej;
                    """        
        fill_table(conn_str, 'groups', create_table_query, copy_query)

    fill_groups_task = fill_groups(conn_str)


    @task()
    def fill_users(conn_str):
        create_table_query = """
                    CREATE TABLE STV2024012236__STAGING.users
                    (
                        id int NOT NULL PRIMARY KEY,
                        chat_name varchar(200),
                        registration_dt timestamp(6),
                        country varchar(200),
                        age int
                    )
                    order by id
                    segmented by hash(id) all nodes;
                    """
        copy_query = """
                    COPY STV2024012236__STAGING.users(id, chat_name, registration_dt, country, age)
                    FROM LOCAL '/data/users.csv'
                    DELIMITER ','
                    ENCLOSED BY '"'
                    REJECTED DATA AS TABLE users_rej;
                    """
        fill_table(conn_str, 'users', create_table_query, copy_query)

    fill_users_task = fill_users(conn_str)


    @task()
    def fill_dialogs(conn_str):
        create_table_query = """
                    CREATE TABLE STV2024012236__STAGING.dialogs
                    (
                        message_id int NOT NULL PRIMARY KEY,
                        message_ts timestamp(6),
                        message_from int,
                        message_to int,
                        message varchar(1000),
                        message_group varchar(100)
                    )
                    order by message_id
                    segmented by hash(message_id) all nodes
                    PARTITION BY message_ts::date
                    GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);
                    """
        copy_query = """
                    COPY STV2024012236__STAGING.dialogs(message_id, message_ts, message_from, message_to, message, message_group)
                    FROM LOCAL '/data/dialogs.csv'
                    DELIMITER ','
                    ENCLOSED BY '"'
                    REJECTED DATA AS TABLE dialogs_rej;
                    """
        fill_table(conn_str, 'dialogs', create_table_query, copy_query)

    fill_dialogs_task = fill_dialogs(conn_str)


    @task()
    def fill_group_log(conn_str):
        create_table_query = """
                    CREATE TABLE STV2024012236__STAGING.group_log
                    (
                        group_id int NOT NULL PRIMARY KEY,
                        user_id int not null,
                        user_id_from int,
                        event varchar(20) not null check (event in ('create', 'add', 'leave')),
                        datetime timestamp(6)
                    )
                    order by group_id
                    segmented by hash(group_id) all nodes
                    PARTITION BY datetime::date
                    GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);
                    """
        copy_query = """
                    COPY STV2024012236__STAGING.group_log(group_id, user_id, user_id_from, event, datetime)
                    FROM LOCAL '/data/group_log.csv'
                    DELIMITER ','
                    ENCLOSED BY '"'
                    REJECTED DATA AS TABLE group_log_rej;
                    """     
        fill_table(conn_str, 'group_log', create_table_query, copy_query)
        
    fill_group_log_task = fill_group_log(conn_str)

    # Добавляем пустой task как заглушку для построения графа
    separator_blank_task = EmptyOperator(task_id='separator_blank_task')

    # Формируем очередность исполнения задач
    [groups, users, dialogs, group_logs] >> separator_blank_task
    separator_blank_task >> [fill_users_task, fill_dialogs_task, fill_groups_task, fill_group_log_task]


sprint6_get_groups_dag = sprint6_get_groups_csv()
