# gold dwh tables
CUSTOMER_DWH_TABLE = {
    'table_name': 'customer'
    ,'database_name': 'gold_dwh'
    ,'columns': {
        'customer_id': 'INTEGER'
        ,'first_name': 'TEXT'
        ,'last_name': 'TEXT'
        ,'email': 'TEXT'
        ,'phone_number': 'TEXT'
        ,'address': 'TEXT'
        ,'city': 'TEXT'
    }
}
PRODUCT_DWH_TABLE = {
    'table_name': 'product'
    ,'database_name': 'gold_dwh'
    ,'columns': {
        'product_id': 'INTEGER'
        ,'product_type': 'TEXT'
        ,'brand': 'TEXT'
        ,'model': 'TEXT'
        ,'price_usd': 'REAL'
        ,'release_year': 'INTEGER'
        ,'color': 'TEXT'
        ,'connector_type': 'TEXT'
        ,'wireless': 'TEXT'
    }
}
SALES_DWH_TABLE = {
    'table_name': 'sale'
    ,'database_name': 'gold_dwh'
    ,'columns': {
        'sale_id': 'INTEGER'
        ,'customer_id': 'INTEGER'
        ,'product_id': 'INTEGER'
        ,'sale_timestamp': 'DATETIME'
    }
}
WEB_VISIT_BRONZE_TABLE = {
    'table_name': 'web_visit'
    ,'database_name': 'bronze'
    ,'columns': {
        'web_visit_id': 'INTEGER'
        ,'customer_id': 'INTEGER'
        ,'visit_id': 'INTEGER'
        ,'web_visit_timestamp': 'DATETIME'
        ,'web_url': 'TEXT'
    }
}