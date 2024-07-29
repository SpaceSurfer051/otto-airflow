import boto3
import pandas as pd
from io import StringIO
import random
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime
import logging


def integrate_data():
    color_list = ["red", "blue", "green", "yellow", "black", "white", "gray", "orange", "purple", "brown",
                "pink", "cyan", "magenta", "lime", "teal", "lavender", "beige", "maroon", "olive", "navy"]

    bucket_name = 'otto-glue'
    folder_name = 'non-integrated-data'

    product_file_names = [
        '29cm_products.csv',
        'processed_products.csv',
        'test_zigzag_product',
    ]
    review_file_names = [
        '29cm_reviews.csv',
        'musinsa_reviews.csv',
        'test_zigzag_reviews',
    ]
    now = datetime.now()
    combined_reviews_file_key = f'integrated-data/reviews/combined_reviews_{now}.csv'
    combined_products_file_key = f'integrated-data/products/combined_products_{now}.csv'

    # S3 연결을 위한 Airflow S3 Hook 생성
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    s3_client = s3_hook.get_conn()

    def read_s3_csv(bucket_name, file_key):
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            df = pd.read_csv(obj['Body'])
            return df
        except Exception as e:
            logging.error(f"Error reading {file_key} from S3: {e}")
            return pd.DataFrame()  # 빈 데이터프레임 반환

    def process_reviews(df, is_zigzag=False):
        if is_zigzag:
            try:
                df.drop(columns=['Unnamed: 0', 'product_id'], inplace=True, errors='ignore')
                df['brightness_comment'] = 'none'
                df['gender'] = 'none'
                df['thickness_comment'] = 'none'
            except KeyError as e:
                logging.error(f"Column not found: {e}")
        return df

    def process_products(df, platform_name, is_zigzag=False):
        if is_zigzag:
            try:
                df.drop(columns=['Unnamed: 0'], inplace=True, errors='ignore')
            except KeyError as e:
                logging.error(f"Column not found: {e}")
        try:
            df['platform'] = platform_name
        except Exception as e:
            logging.error(f"Error adding platform column: {e}")
        return df

    # 리뷰 파일 통합
    review_dfs = []
    for file_name in review_file_names:
        file_key = f'{folder_name}/{file_name}'
        df = read_s3_csv(bucket_name, file_key)
        if 'zigzag' in file_name:
            df = process_reviews(df, is_zigzag=True)
        review_dfs.append(df)

    try:
        combined_reviews_df = pd.concat(review_dfs).drop_duplicates().reset_index(drop=True)
    except ValueError as e:
        logging.error(f"Error concatenating review dataframes: {e}")
        combined_reviews_df = pd.DataFrame()
    
    print("Combined Reviews DataFrame Columns:")
    print(combined_reviews_df.columns)

    # 제품 파일 통합
    product_dfs = []
    platform_mapping = {
        '29cm_products.csv': '29cm',
        'processed_products.csv': 'musinsa',
        'test_zigzag_product': 'zigzag'
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

    try:
        combined_products_df = pd.concat(product_dfs).drop_duplicates().reset_index(drop=True)
    except ValueError as e:
        logging.error(f"Error concatenating product dataframes: {e}")
        combined_products_df = pd.DataFrame()
    
    print("Combined Products DataFrame Columns:")
    print(combined_products_df.columns)

    # 색상 추가
    def add_random_colors(row):
        try:
            if row['color'] == 'none':
                num_colors = random.randint(1, 4)
                row['color'] = ', '.join(random.sample(color_list, num_colors))
        except KeyError as e:
            logging.error(f"Color column not found in row: {e}")
        return row

    try:
        combined_products_df = combined_products_df.apply(add_random_colors, axis=1)
    except Exception as e:
        logging.error(f"Error applying random colors: {e}")

    # S3에 CSV 파일 저장
    def save_to_s3(dataframe, file_key):
        try:
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
            print(f"Saved {file_key} to S3")
        except Exception as e:
            logging.error(f"Error saving {file_key} to S3: {e}")

    save_to_s3(combined_reviews_df, combined_reviews_file_key)
    save_to_s3(combined_products_df, combined_products_file_key)
