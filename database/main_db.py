from datetime import datetime, date
import os
from pathlib import Path
import pandas as pd
import MySQLdb
import psycopg2
from sqlalchemy import text, create_engine
from dotenv import dotenv_values
from loguru import logger
from urllib.parse import quote_plus as urlquote

from sys import platform


config = dotenv_values(f'{os.path.dirname(os.path.abspath(__file__))}/../.env')

today_str = date.today().strftime('%Y%m%d')
today_time_str = datetime.now().strftime('%Y%m%d%H%M%S')

current_path = Path(os.path.abspath(__file__)).parent


class Singleton:

    def __init__(self, cls):
        self._cls = cls

    def Instance(self, **kwargs):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._cls(**kwargs)
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._cls)


class EPDB:

    def __init__(self, dbc, db_type='mysql', schema=None):
        print(dbc)
        print(db_type)
        self.db_type = db_type
        try:
            if db_type == 'mysql':
                self._conn = MySQLdb.connect(**dbc, charset='utf8')
                self.engine = create_engine(
                    f"mysql://{dbc['user']}:{urlquote(dbc['passwd'])}@{dbc['host']}:{dbc['port']}/{dbc['db']}?charset=utf8mb4",
                    echo=False)
                self._cursor = self._conn.cursor()
                self.execute(sql="SET session wait_timeout=1500")

            if db_type == 'postgres':
                con_string = f"postgresql://{dbc['user']}:{(dbc['password'])}@{dbc['host']}:{dbc['port']}/{dbc['dbname']}"

                if schema is not None:
                    # options="-c search_path=dbo,public"
                    dbc['options'] = f"-c search_path={schema}"
                    #  ?options=-csearch_path%3Ddbo,public
                    con_string += f'?options=-csearch_path%3D{schema}'

                self.engine = create_engine(con_string, echo=False)

                self._conn = psycopg2.connect(**dbc)
                self._cursor = self._conn.cursor()

        except Exception as e:
            print(e.__repr__())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    def __str__(self):
        return self._conn

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    @property
    def description(self):
        return self.cursor.description

    # @retry(Exception, tries=5)
    def query(self, sql, params=None, return_dict=False):
        try:
            if self._conn is None:
                self.__init__()
            elif self.db_type == 'mysql':
                self._conn.ping(True)

            ex_result = self.cursor.execute(sql, params or ())
            if sql.lower().split()[0] in ['update', 'insert', 'delete', 'alter', 'create']:
                self.commit()
                return ex_result
        except Exception as e:
            logger.debug(f'{e} : {sql}')
            return None

        if return_dict:
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        return self.cursor.fetchall()

    def query_df(self, sql):
        return pd.read_sql(text(sql), self.engine)

    def df_to_db(self, df, **kwargs):
        df.to_sql(**kwargs, con=self.engine)


def mysql_db():
    host = config['server']
    user = config['mysql_user']
    passwd = config['mysql_password']
    db = config['mysql_db']
    dbc = dict(host=host, user=user, passwd=passwd, db=db, port=3306)
    return EPDB(dbc=dbc, db_type='mysql')


def postgres_db(schema=None):
    host = config['server']
    user = config['user']
    passwd = config['password']
    port = config['port']
    db = config['db_name']
    dbc = dict(host=host, user=user, password=passwd, dbname=db, port=port)
    return EPDB(dbc=dbc, db_type='postgres', schema=schema)


@Singleton
class DB:
    def __init__(self):
        self.pg_db = postgres_db()
        self.mysql_db = mysql_db()


if __name__ == '__main__':
    pass
