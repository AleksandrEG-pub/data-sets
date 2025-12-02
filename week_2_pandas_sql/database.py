from typing import Callable
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from sqlalchemy import Engine
import psycopg2
import pandas as pd
import os
import logging

db_user = "it_one"
db_password = "it_one"
db_name = "it_one"
db_host = "127.0.0.1"
db_port = "10451"

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
tables_sql_file = f"{script_dir}/sql_scripts/db.sql"


def with_connection(func: Callable, sql_query: str):
    connection = None
    try:
        connection = psycopg2.connect(user=db_user,
                                      password=db_password,
                                      dbname=db_name,
                                      host=db_host,
                                      port=db_port)
        result = func(connection, sql_query)
        connection.commit()
        return result
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if connection:
            connection.close()


def execute_sql_update(connection, sql_update: str):
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    cursor.execute(sql_update)
    if cursor.rowcount > 0:
        logging.info(f"updated {cursor.rowcount}")


def setup_database():
    with open(tables_sql_file, 'r') as file:
        sql_commands = ''
        try:
            sql_commands = file.read()
            logging.info("tables created")
        except FileNotFoundError:
            logging.error(f"failed to read init scripts from file {tables_sql_file}")
        with_connection(execute_sql_update, sql_commands)


def df_to_sql(engine: Engine, table: str, df: pd.DataFrame):
    with engine.connect() as conn:
        df = df.to_sql(
            name=table,
            con=conn,
            if_exists='replace',
            index=False
        )


def save_to_db(table_name: str, df: pd.DataFrame):
    connection = None
    try:
        connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        engine: Engine = create_engine(connection_string)
        df_to_sql(engine, table_name, df)
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if connection:
            connection.close()
