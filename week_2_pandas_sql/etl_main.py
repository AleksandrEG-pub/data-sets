import pandas as pd

import etl_customers
import etl_sales
import etl_sales_aggregate
import database
from etl_exception import EtlError

def process_all():
    database.setup_database()
    
    customers_df: pd.DataFrame = etl_customers.extract_process()
    database.save_to_db('customers', customers_df)

    sales_df = etl_sales.extract_process()
    database.save_to_db('sales', sales_df)

    aggregate_df = etl_sales_aggregate.aggregate_categories(sales_df)
    database.save_to_db('sales_summary', aggregate_df)

    product_ranking_df = etl_sales_aggregate.aggregate_top_products(sales_df)
    database.save_to_db('product_ranking', product_ranking_df)

    region_df = etl_sales_aggregate.aggregate_region_check(sales_df, customers_df)
    database.save_to_db('region_summary', region_df)


try:
    process_all()
except EtlError:
    print("application did not finish")