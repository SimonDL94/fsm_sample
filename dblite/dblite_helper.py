import sqlite3
import pandas as pd


def create_database(path):
    conn = sqlite3.connect(path)
    conn.close()
    print("sqlite db created at: ", path)


def execute_sqlite_sql(query, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.close()
    print('done executing query')


def get_sqlite_sql(select_query, db_path):
    conn = sqlite3.connect(db_path)
    # init cursor to access data in database from query
    cursor = conn.cursor()
    cursor.execute(select_query)
    df = pd.DataFrame(cursor.fetchall(), columns=[description[0] for description in cursor.description])
    return df


def overwrite_df_to_sqlite_table(df, table_name, db_path):
    conn = sqlite3.connect(db_path)
    truncate_query = 'DELETE FROM ' + table_name
    print(truncate_query)
    execute_sqlite_sql(truncate_query, db_path)
    df.to_sql(
        table_name
        ,conn
        ,if_exists='append'
        ,index=False
    )
    conn.close()

