from



N_CLIENT_RECORDS = 1000
N_PRODUCT_RECORDS = 10
N_SALE_RECORDS = 20000

def create_test_db():
    db_executor.create_sqlite_db(ma_basis_constants.PATH_TO_SQLITE_DATABASE)
    clients = generate_clients()
    products = generate_products()
    sales = generate_sales(clients, products)
    db_executor.insert_df_into_sqlite(clients, 'client')
    db_executor.insert_df_into_sqlite(products, 'product')
    db_executor.insert_df_into_sqlite(sales, 'sale')




def select_random_value_or_none(value):
    # 1/4 select a None value
    if random.choice([True, True, True, False]):
        out = value
    else:
        out = None
    return out


def select_random_value_from_list(values_list):
    # 1/4 select a None value
    return random.choice(values_list)


def generate_clients(n_records=N_CLIENT_RECORDS):
    possible_values = {
        'gender': ['M', 'F']
    }
    fake = Faker()
    records = []
    for _ in range(n_records):
        first_name = fake.first_name()
        last_name = fake.last_name()
        record = {
            'dateofbirth': fake.date_of_birth(minimum_age=20, maximum_age=80)
            ,'gender': select_random_value_from_list(possible_values['gender'])
            ,'phonenumber': select_random_value_or_none(fake.phone_number())
            ,'email': select_random_value_or_none(
                f"{first_name.lower()}.{last_name.lower()}@example.com"
            )
        }
        records.append(record)
    df = pd.DataFrame(records)
    df.insert(0, 'clientid', range(1, len(df) + 1))
    return df


def generate_products(n_records=N_PRODUCT_RECORDS, min_price = 5, max_price = 30):
    fake = Faker()
    records = []
    for _ in range(n_records):
        record = {
            'name': fake.sentence(nb_words=2).replace('.','')
            ,'price': random.randint(min_price, max_price)
        }
        records.append(record)
    df = pd.DataFrame(records)
    df.insert(0, 'productid', range(1, len(df) + 1))
    return df


def generate_sales(clients_df, products_df, n_records=N_SALE_RECORDS):
    records = []
    years_ago = 5
    five_years_ago = datetime.now() - timedelta(days=365 * years_ago)
    for _ in range(n_records):
        random_time_difference = random.uniform(0, (datetime.now() - five_years_ago).total_seconds())
        saledate = five_years_ago + timedelta(seconds=random_time_difference)
        record = {
            'clientid': random.choice(clients_df['clientid'])
            ,'productid': random.choice(products_df['productid'])
            ,'saledate': saledate
        }
        records.append(record)
    df = pd.DataFrame(records)
    df.insert(0, 'saleid', range(1, len(df) + 1))
    return df


def create_test_db():
    db_executor.create_sqlite_db(ma_basis_constants.PATH_TO_SQLITE_DATABASE)
    clients = generate_clients()
    products = generate_products()
    sales = generate_sales(clients, products)
    db_executor.insert_df_into_sqlite(clients, 'client')
    db_executor.insert_df_into_sqlite(products, 'product')
    db_executor.insert_df_into_sqlite(sales, 'sale')


if __name__ == "__main__":
    create_test_db()