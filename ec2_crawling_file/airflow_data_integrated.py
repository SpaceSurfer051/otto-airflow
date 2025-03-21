import boto3
import pandas as pd
from io import StringIO
import random
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime


def integrate_data():
    color_list = ["red", "blue", "green", "yellow", "black", "white", "gray", "orange", "purple", "brown",
                "pink", "cyan", "magenta", "lime", "teal", "lavender", "beige", "maroon", "olive", "navy"]

    bucket_name = 'otto-glue'
    folder_name = 'non-integrated-data'

    product_file_names = [
        '29cm_products.csv',
        'processed_products.csv',
        'zigzag_products.csv',
    ]
    review_file_names = [
        '29cm_reviews.csv',
        'musinsa_reviews.csv',
        'zigzag_reviews.csv'
    ]
    now = datetime.now()
    combined_reviews_file_key = f'integrated-data/reviews/combined_reviews_{now}.csv'
    combined_products_file_key = f'integrated-data/products/combined_products_{now}.csv'

    # S3 연결을 위한 Airflow S3 Hook 생성
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    s3_client = s3_hook.get_conn()

    def read_s3_csv(bucket_name, file_key):
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'])
        return df

    def process_reviews(df, is_zigzag=False):
        if is_zigzag:
            df.drop(columns=['Unnamed: 0', 'product_id'], inplace=True, errors='ignore')
            df['brightness_comment'] = 'none'
            df['gender'] = 'none'
            df['thickness_comment'] = 'none'
        return df

    def process_products(df, platform_name, is_zigzag=False):
        if is_zigzag:
            df.drop(columns=['Unnamed: 0'], inplace=True, errors='ignore')
        df['platform'] = platform_name
        return df

    # 리뷰 파일 통합
    review_dfs = []
    for file_name in review_file_names:
        file_key = f'{folder_name}/{file_name}'
        df = read_s3_csv(bucket_name, file_key)
        if 'zigzag' in file_name:
            df = process_reviews(df, is_zigzag=True)
        review_dfs.append(df)

    combined_reviews_df = pd.concat(review_dfs).drop_duplicates().reset_index(drop=True)
    print("Combined Reviews DataFrame Columns:")
    print(combined_reviews_df.columns)

    # 제품 파일 통합
    product_dfs = []
    platform_mapping = {
        '29cm_products.csv': '29cm',
        'processed_products.csv': 'musinsa',
        'zigzag_products.csv': 'zigzag'
    }

    for file_name in product_file_names:
        file_key = f'{folder_name}/{file_name}'
        df = read_s3_csv(bucket_name, file_key)
        platform_name = platform_mapping[file_name]
        if 'zigzag' in file_name:
            df = process_products(df, platform_name, is_zigzag=True)
        else:
            df = process_products(df, platform_name)
        product_dfs.append(df)

    combined_products_df = pd.concat(product_dfs).drop_duplicates().reset_index(drop=True)
    print("Combined Products DataFrame Columns:")
    print(combined_products_df.columns)

    # 색상 추가
    def add_random_colors(row):
        if row['color'] == 'none':
            num_colors = random.randint(1, 4)
            row['color'] = ', '.join(random.sample(color_list, num_colors))
        return row

    combined_products_df = combined_products_df.apply(add_random_colors, axis=1)

    # S3에 CSV 파일 저장
    def save_to_s3(dataframe, file_key):
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
        print(f"Saved {file_key} to S3")

    save_to_s3(combined_reviews_df, combined_reviews_file_key)
    save_to_s3(combined_products_df, combined_products_file_key)
