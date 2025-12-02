import pandas as pd
from datetime import datetime
import etl_csv

customers_file = './data/customers.csv'
wrong_emails_file = './result/wrong_emails_file.csv'


def handle_wrong_email(df: pd.DataFrame):
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    email_filter = df['email'].str.match(email_pattern, na=False)
    wrong_email_df = df[~email_filter]
    etl_csv.write_to_csv(wrong_email_df, wrong_emails_file)
    return df[email_filter]


def days_difference(date_string, date_format="%Y-%m-%d"):
    given_date = datetime.strptime(date_string, date_format)
    current_date = datetime.now()
    difference = current_date - given_date
    return str(difference.days)


def add_customer_days(df: pd.DataFrame):
    df = df.copy()
    df['customer_days'] = df['registration_date'].apply(days_difference)
    return df


def transform_customers(df: pd.DataFrame):
    df = handle_wrong_email(df)
    df = add_customer_days(df)
    return df


def extract_process() -> pd.DataFrame:
    customers_df = etl_csv.read_csv(customers_file)
    customers_df = transform_customers(customers_df)
    return customers_df
    