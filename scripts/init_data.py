import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from dblite import dblite_generator, dblite_helper


def generate_data():
    dblite_generator.generate_data()
    df = dblite_helper.get_sqlite_sql('SELECT * FROM customer', dblite_generator.GOLD_DATABASE_PATH)
    print('gold - customer table shape ' + str(df.shape))
    print(df.dtypes)
    df = dblite_helper.get_sqlite_sql('SELECT * FROM product', dblite_generator.GOLD_DATABASE_PATH)
    print('gold - product table shape ' + str(df.shape))
    print(df.dtypes)
    df = dblite_helper.get_sqlite_sql('SELECT * FROM sale', dblite_generator.GOLD_DATABASE_PATH)
    print('gold - sale table shape ' + str(df.shape))
    print(df.dtypes)
    df = dblite_helper.get_sqlite_sql('SELECT * FROM web_visit', dblite_generator.BRONZE_DATABASE_PATH)
    print('bronze - web_visit table shape ' + str(df.shape))
    print(df.dtypes)


generate_data()
