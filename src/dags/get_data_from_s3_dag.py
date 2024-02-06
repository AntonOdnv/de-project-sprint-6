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

class get_data_from_s3:

    def __init__(self, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, endpoint_url):
        self.AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
        self.AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY
        self.endpoint_url = endpoint_url

    def get_data(self, bucket, key, filename):

        session = boto3.session.Session()

        s3_client = session.client(
            service_name = 's3',
            endpoint_url = self.endpoint_url,
            aws_access_key_id = self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = self.AWS_SECRET_ACCESS_KEY,
        )

        s3_client.download_file(
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

    s3 = get_data_from_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, endpoint_url)

    @task()
    def get_groups():
        s3.get_data('sprint6', 'groups.csv', '/data/groups.csv')

    groups = get_groups()

    @task()
    def get_users():
        s3.get_data('sprint6', 'users.csv', '/data/users.csv')

    users = get_users()

    @task()
    def get_dialogs():
        s3.get_data('sprint6', 'dialogs.csv', '/data/dialogs.csv')
        
    dialogs = get_dialogs()

    @task()
    def get_log():
        s3.get_data('sprint6', 'group_log.csv', '/data/group_log.csv')
        
    group_logs = get_log()

    @task()
    def fill_groups(conn_str):
        with vertica_python.connect(**conn_str) as conn:

            querry1 = """
            DROP TABLE IF EXISTS STV2024012236__STAGING.groups;
            """
            querry2 = """
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
            querry3 = """
            COPY STV2024012236__STAGING.groups(id, admin_id, group_name, registration_dt, is_private)
            FROM LOCAL '/data/groups.csv'
            DELIMITER ','
            ENCLOSED BY '"'
            REJECTED DATA AS TABLE groups_rej;
            """
            cur = conn.cursor()
            cur.execute(querry1)
            cur.execute(querry2)
            cur.execute(querry3)
        
    fill_groups_task = fill_groups(conn_str)

    @task()
    def fill_users(conn_str):
        with vertica_python.connect(**conn_str) as conn:

            querry1 = """
            DROP TABLE IF EXISTS STV2024012236__STAGING.users;
            """
            querry2 = """
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
            querry3 = """
            COPY STV2024012236__STAGING.users(id, chat_name, registration_dt, country, age)
            FROM LOCAL '/data/users.csv'
            DELIMITER ','
            ENCLOSED BY '"'
            REJECTED DATA AS TABLE users_rej;
            """
            cur = conn.cursor()
            cur.execute(querry1)
            cur.execute(querry2)
            cur.execute(querry3)
        
    fill_users_task = fill_users(conn_str)

    @task()
    def fill_dialogs(conn_str):
        with vertica_python.connect(**conn_str) as conn:

            querry1 = """
            DROP TABLE IF EXISTS STV2024012236__STAGING.dialogs;
            """
            querry2 = """
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
            querry3 = """
            COPY STV2024012236__STAGING.dialogs(message_id, message_ts, message_from, message_to, message, message_group)
            FROM LOCAL '/data/dialogs.csv'
            DELIMITER ','
            ENCLOSED BY '"'
            REJECTED DATA AS TABLE dialogs_rej;
            """
            cur = conn.cursor()
            cur.execute(querry1)
            cur.execute(querry2)
            cur.execute(querry3)
        
    fill_dialogs_task = fill_dialogs(conn_str)

    @task()
    def fill_group_logs(conn_str):
        with vertica_python.connect(**conn_str) as conn:

            querry1 = """
            DROP TABLE IF EXISTS STV2024012236__STAGING.group_log;
            """
            querry2 = """
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
            querry3 = """
            COPY STV2024012236__STAGING.group_log(group_id, user_id, user_id_from, event, datetime)
            FROM LOCAL '/data/group_log.csv'
            DELIMITER ','
            ENCLOSED BY '"'
            REJECTED DATA AS TABLE group_log_rej;
            """
            cur = conn.cursor()
            cur.execute(querry1)
            cur.execute(querry2)
            cur.execute(querry3)
        
    fill_group_logs_task = fill_group_logs(conn_str)

    separator_blank_task = EmptyOperator(task_id='separator_blank_task')

    [groups, users, dialogs, group_logs] >> separator_blank_task
    separator_blank_task >> [fill_users_task, fill_dialogs_task, fill_groups_task, fill_group_logs_task]

sprint6_get_groups_dag = sprint6_get_groups_csv()