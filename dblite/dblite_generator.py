import pandas as pd
from faker import Faker
import os
import sys
from datetime import datetime, timedelta
import random
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from dblite import dblite_helper, dblite_tables


DATA_RELATIVE_PATH = '/_data'
BRONZE_DATABASE_PATH = os.path.dirname(
    os.path.dirname(os.path.realpath(__file__))
) + DATA_RELATIVE_PATH + '/bronze.sqlite'
GOLD_DATABASE_PATH = os.path.dirname(
    os.path.dirname(os.path.realpath(__file__))
) + DATA_RELATIVE_PATH + '/gold_dwh.sqlite'


folder_path = "/path/to/your/folder"


def clear_dbs():
    try:
        os.remove(BRONZE_DATABASE_PATH)
        os.remove(GOLD_DATABASE_PATH)
    except:
        print('unable to remove databases')


def create_dbs():
    print(BRONZE_DATABASE_PATH)
    dblite_helper.create_database(BRONZE_DATABASE_PATH)
    dblite_helper.create_database(GOLD_DATABASE_PATH)
    print('done creating sqlite databases')


def generate_create_table_script(table_dictionary):
    table_name = table_dictionary['table_name']
    columns = table_dictionary['columns']

    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("

    for column_name, column_type in columns.items():
        create_table_query += f"{column_name} {column_type}, "

    create_table_query = create_table_query.rstrip(', ')
    create_table_query += ");"

    return create_table_query


def get_database_path(database_name):
    if database_name == 'bronze':
        return BRONZE_DATABASE_PATH
    elif database_name == 'gold_dwh':
        return GOLD_DATABASE_PATH
    else:
        return None


def create_single_table(table_dictionary):
    query = generate_create_table_script(table_dictionary)
    dblite_helper.execute_sqlite_sql(query, get_database_path(table_dictionary['database_name']))


def create_tables():
    create_single_table(dblite_tables.CUSTOMER_DWH_TABLE)
    create_single_table(dblite_tables.PRODUCT_DWH_TABLE)
    create_single_table(dblite_tables.SALES_DWH_TABLE)
    create_single_table(dblite_tables.WEB_VISIT_BRONZE_TABLE)
    print('done creating tables')


def generate_customer_data(n_records=10000):
    fake = Faker('nl_NL')
    customer_data = {
        'first_name': [],
        'last_name': [],
        'email': [],
        'phone_number': [],
        'address': [],
        'city': [],
    }
    for _ in range(n_records):
        customer_data['first_name'].append(fake.first_name())
        customer_data['last_name'].append(fake.last_name())
        customer_data['email'].append(fake.email())
        customer_data['phone_number'].append(fake.phone_number())
        customer_data['address'].append(fake.street_address())
        customer_data['city'].append(fake.city())
    # return customer dataframe
    df = pd.DataFrame(customer_data)
    df['customer_id'] = df.reset_index().index
    return df


def fill_customer_table(n_records=1000):
    table_dictionary = dblite_tables.CUSTOMER_DWH_TABLE
    df = generate_customer_data(n_records)
    dblite_helper.overwrite_df_to_sqlite_table(
        df
        ,table_dictionary['table_name']
        ,get_database_path(table_dictionary['database_name'])
    )


def generate_product_data(n_records=10000):
    fake = Faker()

    product_data = {
        'product_type': [],
        'brand': [],
        'model': [],
        'price_usd': [],
        'release_year': [],
        'color': [],
        'connector_type': [],
        'wireless': [],
    }

    for _ in range(n_records):
        product_types = ['Headphones', 'Smartphone', 'Laptop', 'Tablet', 'Smartwatch']
        product_data['product_type'].append(fake.random_element(elements=product_types))
        product_data['brand'].append(fake.company())
        product_data['model'].append(fake.random_element(elements=('Elite', 'Pro', 'Max', 'Ultimate', 'Epic')))
        product_data['price_usd'].append(fake.random_int(min=20, max=1000))
        product_data['release_year'].append(fake.random_int(min=2015, max=2023))
        product_data['color'].append(fake.color_name())
        product_data['connector_type'].append(fake.random_element(elements=('3.5mm', 'Bluetooth', 'USB-C')))
        product_data['wireless'].append(fake.random_element(elements=('Yes', 'No')))
    df = pd.DataFrame(product_data)
    df['product_id'] = df.reset_index().index
    return df


def fill_product_table(n_records=1000):
    table_dictionary = dblite_tables.PRODUCT_DWH_TABLE
    df = generate_product_data(n_records)
    dblite_helper.overwrite_df_to_sqlite_table(
        df
        ,table_dictionary['table_name']
        ,get_database_path(table_dictionary['database_name'])
    )


def generate_sales_data(n_records=10000, n_years_ago=5):
    all_customers_ids = dblite_helper.get_sqlite_sql('SELECT customer_id FROM customer', GOLD_DATABASE_PATH)[
        'customer_id'].tolist()
    all_product_ids = dblite_helper.get_sqlite_sql('SELECT product_id FROM product', GOLD_DATABASE_PATH)[
        'product_id'].tolist()
    sales_data = {
        'customer_id': [],
        'product_id': [],
        'sale_timestamp': []
    }

    for _ in range(n_records):
        sales_data['customer_id'].append(random.choice(all_customers_ids))
        sales_data['product_id'].append(random.choice(all_product_ids))
        random_days_ago = random.randint(0, 365 * n_years_ago)
        sales_data['sale_timestamp'].append(datetime.now() - timedelta(days=random_days_ago))
    df = pd.DataFrame(sales_data)
    df['sale_id'] = df.reset_index().index
    return df


def fill_sale_table(n_records=200000, n_years_ago=5):
    table_dictionary = dblite_tables.SALES_DWH_TABLE
    df = generate_sales_data(n_records, n_years_ago)
    dblite_helper.overwrite_df_to_sqlite_table(
        df
        ,table_dictionary['table_name']
        ,get_database_path(table_dictionary['database_name'])
    )


def generate_web_visits_data(n_records=100000, n_years_ago=3):
    fake = Faker()
    all_customers_ids = dblite_helper.get_sqlite_sql('SELECT customer_id FROM customer', GOLD_DATABASE_PATH)[
        'customer_id'].tolist()
    all_web_urls = []
    # max 350 number of possible urls
    base_url = "https://www.fsm_example_.com"
    for web_url in range(0, 180):
        fake_url = f"{base_url}/{fake.uri_path()}"
        all_web_urls.append(fake_url)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * n_years_ago)
    web_visit_data = {
        'web_visit_id': [],
        'customer_id': [],
        'visit_id': [],
        'web_visit_timestamp': [],
        'web_url': []
    }
    # visit interval each 5 minutes
    smallest_interval = timedelta(minutes=5)
    last_visit_timestamp = start_date
    last_visit_id = 1
    last_customer_id = random.choice(all_customers_ids)
    for _ in range(n_records):
        if last_visit_timestamp < end_date:
            last_visit_timestamp = last_visit_timestamp + smallest_interval
            web_visit_data['web_visit_timestamp'].append(last_visit_timestamp)
            if random.random() < 0.5:
                web_visit_data['visit_id'].append(last_visit_id)
                # to illustrate data quality issues
                if random.random() < 0.05:
                    web_visit_data['customer_id'].append(-1)
                else:
                    web_visit_data['customer_id'].append(last_customer_id)
            else:
                last_customer_id = random.choice(all_customers_ids)
                last_visit_id = last_visit_id + 1
                web_visit_data['visit_id'].append(last_visit_id)
                web_visit_data['customer_id'].append(last_customer_id)
            web_visit_data['web_url'].append(random.choice(all_web_urls))
        else:
            break
    df = pd.DataFrame(
        web_visit_data
        , columns=['customer_id', 'visit_id', 'web_visit_timestamp', 'web_url']
    )
    df['web_visit_id'] = df.reset_index().index
    return df


def fill_web_visit_table(n_records=200000, n_years_ago=5):
    table_dictionary = dblite_tables.WEB_VISIT_BRONZE_TABLE
    df = generate_web_visits_data(n_records, n_years_ago)
    dblite_helper.overwrite_df_to_sqlite_table(
        df
        ,table_dictionary['table_name']
        ,get_database_path(table_dictionary['database_name'])
    )


def generate_data():
    clear_dbs()
    create_dbs()
    create_tables()
    N_CUSTOMER_RECORDS = 30000
    fill_customer_table(N_CUSTOMER_RECORDS)
    N_PRODUCT_RECORDS = 500
    fill_product_table(N_PRODUCT_RECORDS)
    N_SALES_RECORDS = 200000
    SALES_N_YEARS_AGO = 5
    fill_sale_table(N_SALES_RECORDS, SALES_N_YEARS_AGO)
    N_WEB_VISIT_RECORDS = 30000
    WEB_VISIT_N_YEARS_AGO = 5
    fill_web_visit_table(N_WEB_VISIT_RECORDS, WEB_VISIT_N_YEARS_AGO)


if __name__ == "__main__":
    generate_data()