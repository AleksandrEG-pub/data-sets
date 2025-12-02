import pandas as pd
import etl_csv
import logging

sales_file = './data/sales.csv'
missing_values_file = './result/missing_values_file.csv'
duplicate_orders_file = './result/duplicate_orders_file.csv'


def extract_month(date: str):
    date_parts = date.split('-')
    if len(date_parts) == 3:
        return date_parts[1]
    return ''


def set_total_price(df: pd.DataFrame):
    df['total_price'] = df['quantity'] + df['unit_price']
    return df


def set_month_of_the_sale(df: pd.DataFrame):
    df['month'] = df['order_date'].map(extract_month)
    return df


def handle_duplicates(df: pd.DataFrame):
    duplicates_df = df[df.duplicated(subset=['order_id'])]
    if duplicates_df.size > 0:
        logging.info(
            f"Duplicate order_ids: {duplicates_df['order_id'].unique().tolist()}")
        etl_csv.write_to_csv(duplicates_df, duplicate_orders_file)
        return df.drop_duplicates(subset=['order_id'])
    else:
        logging.info("no duplicate orders")


def handle_missing_values(df: pd.DataFrame):
    with_missing_filter = df.isna().any(axis=1)
    df_missing_values = df[with_missing_filter]
    etl_csv.write_to_csv(df_missing_values, missing_values_file)
    return df[~with_missing_filter]


def extract_process():
    df = etl_csv.read_csv(sales_file)
    df = df.copy()
    df = set_total_price(df)
    df = set_month_of_the_sale(df)
    df = handle_duplicates(df)
    df = handle_missing_values(df)
    return df
