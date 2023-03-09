import json
import sqlite3
import pandas as pd
from datetime import datetime, time


class SqliteDB:
    def __init__(self):
        self.conn = sqlite3.connect("D:\Projects\AS Paper\store\AlgoSuccess.db", detect_types=sqlite3.PARSE_DECLTYPES)
        self.db = self.conn.cursor()

    def checkTableExists(self, table_name: str) -> bool:
        table_data = self.conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' "
                                       f"AND name ='{table_name}'").fetchall()
        if table_data:
            return True
        else:
            return False

    def createTable(self, table_name: str, columns: tuple):
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {columns} ")
        self.conn.commit()

    def insertData(self, table_name: str, data: list or dict):
        df = pd.DataFrame.from_dict(data)
        df.to_sql(table_name, self.conn, if_exists='replace', index=True)
        self.conn.commit()

    def findData(self, table_name: str, column: str, condition: str = None):
        if condition is None:
            data = self.conn.execute(f"SELECT {column} FROM {table_name}").fetchall()
            column_name = self.getColumnNames(table_name=table_name)
            data = [dict(zip(column_name, strategy_data)) for strategy_data in data]
            return data
        else:
            data = self.conn.execute(f"SELECT {column} FROM {table_name} WHERE {condition}").fetchall()
            column_name = self.getColumnNames(table_name=table_name)
            data = [dict(zip(column_name, strategy_data)) for strategy_data in data]
            return data

    def getColumnNames(self, table_name: str):
        data = self.conn.execute(f"PRAGMA table_info({table_name})")
        data = data.fetchall()
        return [col[1] for col in data]


if __name__ == '__main__':
    data = [{'Code': 'STR008', 'name': 'sandeep'}, {'Code': 'STR008', 'name': 'sandeep'}]
    print(SqliteDB().insertData(table_name='stf', data=data))
