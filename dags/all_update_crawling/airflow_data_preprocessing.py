import boto3
import pandas as pd
import io
import re
import json
from airflow.hooks.S3_hook import S3Hook

def parse_reviews(review_str):
    try:
        return json.loads(review_str.replace("'", '"'))
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []

def split_weight_height_gender(whg):
    parts = whg.split(' · ')
    if len(parts) == 3:
        gender, height, weight = parts
        height = height.replace('cm', '')
        weight = weight.replace('kg', '')
        return gender.strip(), height.strip(), weight.strip()
    return None, None, None

def save_to_s3(dataframe, bucket_name, file_key, s3_client):
    try:
        csv_buffer = io.StringIO()
        dataframe.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
        print(f"Saved {file_key} to S3")
    except Exception as e:
        print(f"Error saving {file_key} to S3: {e}")

def append_to_existing_s3_csv(existing_df, new_df, bucket_name, file_key, s3_client):
    try:
        combined_df = pd.concat([existing_df, new_df]).drop_duplicates().reset_index(drop=True)
        csv_buffer = io.StringIO()
        combined_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
        print(f"Updated {file_key} in S3")
    except Exception as e:
        print(f"Error updating {file_key} in S3: {e}")

def data_processing():
    '''
    이 코드는 내 전처리 파트임.
    '''
    # AWS S3 버킷 정보
    bucket_name = 'otto-glue'
    file_key = 'non-integrated-data/products_with_size_color.csv'
    reviews_file_key = 'non-integrated-data/musinsa_reviews.csv'
    products_file_key = 'non-integrated-data/processed_products.csv'

    # S3 연결을 위한 Airflow S3 Hook 생성
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    s3_client = s3_hook.get_conn()

    # S3에서 CSV 파일 다운로드
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(content))
    except Exception as e:
        print(f"Error reading S3 object: {e}")
        return

    try:
        # Production preprocessing
        df['product_id'] = 'none'
        df.insert(1, 'rank', [i for i in range(len(df))])

        # price 열의 데이터를 전처리
        df['price'] = df['price'].apply(lambda x: re.sub(r'[^\d,]', '', x.split(' ')[0]))

        # color_options 컬럼 삭제 및 color 컬럼 생성
        if 'color_options' in df.columns:
            df.drop(columns=['color_options'], inplace=True)
        df['color'] = 'none'
        # 이 부분에 color 데이터를 랜덤으로 채우게끔 해야함
        
        
        
        
        
        # category를 전부 'top'으로 바꾸기
        df['category'] = 'top'

        # size_options 컬럼 삭제
        if 'size_options' in df.columns:
            df.drop(columns=['size_options'], inplace=True)

        # 모든 리뷰를 담을 리스트
        all_reviews = []

        # 각 product_name별로 리뷰를 추출하여 통합
        for index, row in df.iterrows():
            product_name = row['product_name']
            reviews_list = parse_reviews(row['reviews'])
            for review in reviews_list:
                review['product_name'] = product_name
                all_reviews.append(review)

        # 통합 리뷰 데이터를 데이터프레임으로 변환
        reviews_df = pd.DataFrame(all_reviews)

        # weight, height, gender를 분리하여 새로운 컬럼 생성
        reviews_df['gender'], reviews_df['height'], reviews_df['weight'] = zip(*reviews_df['weight_height_gender'].map(split_weight_height_gender))

        # weight_height_gender 컬럼 삭제
        reviews_df.drop(columns=['weight_height_gender'], inplace=True)

        # 컬럼명 변경
        reviews_df.rename(columns={'top_size': 'size_comment', 'purchased_size': 'size'}, inplace=True)

        # 불필요한 컬럼 삭제 및 새로운 컬럼 생성
        if 'purchased_product_id' in reviews_df.columns:
            reviews_df.drop(columns=['purchased_product_id'], inplace=True)
        reviews_df['quality_comment'] = 'none'
        reviews_df['color'] = 'none'
        reviews_df['top_size'] = 'none'
        reviews_df['bottom_size'] = 'none'
        df.drop(columns=['reviews'], inplace=True)
        #if 'product_id' in df.columns:
            #df.drop(columns=['product_id'], inplace=True)
            
            

        
        # 기존 리뷰 데이터를 S3에서 불러와서 새 데이터와 병합
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=reviews_file_key)
            existing_reviews_df = pd.read_csv(io.StringIO(response['Body'].read().decode('utf-8')))
            reviews_df = pd.concat([existing_reviews_df, reviews_df]).drop_duplicates().reset_index(drop=True)
        except s3_client.exceptions.NoSuchKey:
            print(f"{reviews_file_key} does not exist. A new file will be created.")
        
        # S3에 통합 리뷰 데이터를 저장
        save_to_s3(reviews_df, bucket_name, reviews_file_key, s3_client)

        # 기존 제품 데이터를 S3에서 불러와서 새 데이터와 병합
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=products_file_key)
            existing_products_df = pd.read_csv(io.StringIO(response['Body'].read().decode('utf-8')))
            df = pd.concat([existing_products_df, df]).drop_duplicates().reset_index(drop=True)
        except s3_client.exceptions.NoSuchKey:
            print(f"{products_file_key} does not exist. A new file will be created.")
        
        # 변경된 product 테이블도 S3에 저장
        save_to_s3(df, bucket_name, products_file_key, s3_client)
    except Exception as e:
        print(f"Error during data processing: {e}")

if __name__ == '__main__':
    data_processing()
