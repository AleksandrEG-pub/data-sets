import pandas as pd


def aggregate_categories(df: pd.DataFrame) -> pd.DataFrame:
    df['order_date'] = pd.to_datetime(df['order_date'], format="%Y-%m-%d")
    result = df.groupby('category', as_index=False).agg(
        total_sales=('total_price', 'sum'),
        total_quantity=('quantity', 'sum'),
        period_date_start=('order_date', 'min'),
        period_date_end=('order_date', 'max')
    )
    result['average_order_value'] = result['total_sales'] / \
        result['total_quantity']
    return result


def aggregate_region_check(sales_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    merged_df = sales_df.merge(customers_df[['customer_id', 'region']],
                               on='customer_id',
                               how='left')
    merged_df.groupby('region')
    average_payment_by_region = merged_df.groupby('region').agg(
        quantity_sold=('quantity', 'sum'),
        total_paid=('total_price', 'sum')
    ).reset_index()
    return average_payment_by_region


def aggregate_top_products(sales_df: pd.DataFrame) -> pd.DataFrame:
    result = sales_df.groupby(['product_id', 'product_name'], as_index=False).agg(
        total_sold=('quantity', 'sum'),
        total_revenue=('total_price', 'sum')
    )
    result['rank_position'] = result['total_revenue'].rank(
        ascending=False, method='dense').astype(int)
    result = result[result['rank_position'] < 6]
    result = result.sort_values('rank_position', ascending=True)
    return result
