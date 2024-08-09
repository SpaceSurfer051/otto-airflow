# musinsa_preprocessing_task.py

from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import re
import random
import json
import pdb
import re
import numpy as np
import random
import ast

def fetch_data_from_redshift(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    conn = redshift_hook.get_conn()
    sql_product = """
        SELECT product_name, size, category, platform, brand
        FROM otto.product_table
        WHERE platform = 'musinsa';
    """
    sql_reviews = """
        SELECT product_name, size, height, weight, gender, size_comment
        FROM otto.reviews
        WHERE product_name IN (
            SELECT product_name
            FROM otto.product_table
            WHERE platform = 'musinsa'
        );
    """

    product_df = pd.read_sql(sql_product, conn)
    reviews_df = pd.read_sql(sql_reviews, conn)

    # Push DataFrames to XCom
    ti = kwargs["ti"]
    ti.xcom_push(key="product_df", value=product_df.to_json())
    ti.xcom_push(key="reviews_df", value=reviews_df.to_json())


def process_data(**kwargs):
    
    # product_size를 리스트로 변환하고 조건에 맞게 처리하는 함수
    def preprocess_product_size(product_size):
        try:
            size_list = ast.literal_eval(product_size)
            if any(len(size) >= 3 for size in size_list):
                return default_sizes
            return size_list
        except:
            return default_sizes

    # 한글과 괄호로 둘러싸인 텍스트를 모두 제거하는 함수
    def clean_size(text):
        no_korean = remove_korean(text)
        no_parentheses = remove_parentheses(no_korean)
        cleaned_text = no_parentheses.strip()  # 앞뒤 공백 제거
        cleaned_text = remove_empty_strings(cleaned_text)
        
        return cleaned_text

    # 한글을 제거하는 함수
    def remove_korean(text):
        return re.sub('[\u3131-\u3163\uac00-\ud7a3]+', '', text)

    # 괄호로 시작해서 괄호로 끝나는 텍스트를 제거하는 함수
    def remove_parentheses(text):
        return re.sub(r'\([^)]*\)', '', text)

    # 빈 문자열을 제거하는 함수
    def remove_empty_strings(size_list_str):
        try:
            # 문자열을 리스트로 변환
            size_list = ast.literal_eval(size_list_str)
            
            # 빈 문자열을 제거
            filtered_list = [size for size in size_list if size.strip() != '']
            
            # 리스트를 다시 문자열로 변환
            return str(filtered_list)
        except:
            # 변환 중 오류가 발생하면 원래 문자열을 반환
            return size_list_str

    def clean_review_size(review_size):
        # 소문자로 변환
        review_size_upper = review_size.upper()
        
        # 겹치는 단어를 찾기
        matched_sizes = [size for size in valid_sizes if size in review_size_upper]
        
        if matched_sizes:
            # 겹치는 단어가 있으면 그 단어만 남기고 반환
            
            if len(matched_sizes) == 1:        
                return ', '.join(matched_sizes)
            else:
                return matched_sizes[-1]
        else:
            # 겹치는 단어가 없으면 한글과 특수문자를 제거하고 반환
            review_size_cleaned = re.sub('[\u3131-\u3163\uac00-\ud7a3]+', '', review_size_upper)
            review_size_cleaned = re.sub(r'[^a-zA-Z0-9]', '', review_size_cleaned)
            return review_size_cleaned

    # review_size를 정리하는 함수
    def clean_review_size_2(row):
        review_size = row['review_size']
        product_size = row['product_size']
        valid_sizes_2 = product_size
            
        # review_size가 None이거나 빈 문자열인 경우 랜덤한 값 선택
        if review_size.strip().upper() == 'NONE' or review_size.strip() == '':  
            return random.choice(valid_sizes_2) if valid_sizes_2 else None
        
        review_size_upper = review_size.upper()
        
        matched_sizes = [size for size in valid_sizes_2 if size in review_size_upper]
        
        if matched_sizes:
            # 겹치는 단어가 있으면 그 단어만 남기고 반환
            if len(matched_sizes) == 1:        
                return ', '.join(matched_sizes)
            else:
                return matched_sizes[-1]
        else:
            # 겹치는 단어가 없으면 한글과 특수문자를 제거하고 반환
            review_size_cleaned = re.sub('[\u3131-\u3163\uac00-\ud7a3]+', '', review_size_upper)
            review_size_cleaned = re.sub(r'[^a-zA-Z0-9]', '', review_size_cleaned)
            return random.choice(valid_sizes_2) if valid_sizes_2 else review_size_cleaned


    # size_comment를 정리하는 함수
    def clean_size_comment(size_comment):
        if pd.isna(size_comment) or size_comment.strip().lower() == 'none':
            return 0
        elif size_comment == '보통이에요' or size_comment ==  '잘 맞아요':
            return 0
        elif size_comment == '작아요':
            return -1
        elif size_comment == '커요':
            return 1
        else:
            return size_comment
        
    # height와 weight를 계산하는 함수
    def calculate_height_weight(row):
        product_size = row['product_size']
        review_size = row['review_size']
        gender = row['gender']
        size_comment = row['size_comment']
        
        valid_sizes_2 = product_size
        size_index = valid_sizes_2.index(review_size) if review_size in valid_sizes_2 else -1
        
        if gender == '남성':
            base_height = 160
            base_weight = 50
            max_height = 190
            max_weight = 100
        else:
            base_height = 150
            base_weight = 40
            max_height = 170
            max_weight = 70
            
        
        height_increment = (max_height - base_height) / (len(valid_sizes_2) - 1) if len(valid_sizes_2) > 1 else 0
        weight_increment = (max_weight - base_weight) / (len(valid_sizes_2) - 1) if len(valid_sizes_2) > 1 else 0
        
        height = base_height + height_increment * size_index
        weight = base_weight + weight_increment * size_index

        if size_comment == -1:
            height -= 3
            weight -= 3
        elif size_comment == 1:
            height += 3
            weight += 3
            
        return round(height), round(weight)


    # gender를 추정하는 함수
    def infer_gender(row):
        product_size = row['product_size']
        review_size = row['review_size']
        
        valid_sizes_2 = product_size
        
        if review_size.lower() in valid_sizes_2:
            index = valid_sizes_2.index(review_size.lower())
            if index >= len(valid_sizes_2) / 2:
                return '남성'
            else:
                return '여성'
        else:
            return '남성' if 'm' in valid_sizes_2 or 'l' in valid_sizes_2 else '여성'
        
        
    ti = kwargs["ti"]
    product_df_json = ti.xcom_pull(key="product_df")
    reviews_df_json = ti.xcom_pull(key="reviews_df")

    product_df = pd.read_json(product_df_json)
    reviews_df = pd.read_json(reviews_df_json)

# culonculon 

    valid_sizes = ['WS', 'XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL', '2XL', '3XL', 'F', 'Free']
    default_sizes = ['XXS', 'XS', 'S', 'M', 'L', 'XL', 'XXL']

    # df1_filtered의 size 컬럼 이름 변경
    df1_filtered = product_df.rename(columns={'size': 'product_size'})

    # df2_filtered의 size 컬럼 이름 변경
    df2_filtered = reviews_df.rename(columns={'size': 'review_size'})

    # product_size 컬럼에서 한글과 괄호로 둘러싸인 텍스트를 모두 제거
    df1_filtered['product_size'] = df1_filtered['product_size'].apply(clean_size)

    df2_filtered.loc[df2_filtered['review_size'].apply(len) >= 3, 'review_size'] = df2_filtered.loc[df2_filtered['review_size'].apply(len) >= 3, 'review_size'].apply(clean_review_size)

    # 두 데이터 프레임을 product_name을 기준으로 조인
    merged_df = pd.merge(df1_filtered, df2_filtered, on='product_name', suffixes=('_product', '_review'))

    # product_size 전처리
    merged_df['product_size'] = merged_df['product_size'].apply(preprocess_product_size)

    # review_size를 전처리
    merged_df['review_size'] = merged_df.apply(clean_review_size_2, axis=1)

    # size_comment 전처리
    merged_df['size_comment'] = merged_df['size_comment'].apply(clean_size_comment)

    # height와 weight를 전처리
    for index, row in merged_df.iterrows():
        if pd.isna(row['height']) or row['height'] == 'none':
            height, weight = calculate_height_weight(row)
            merged_df.at[index, 'height'] = height
            merged_df.at[index, 'weight'] = weight
            
    # gender 전처리
    merged_df['gender'] = merged_df.apply(lambda row: infer_gender(row) if pd.isna(row['gender']) else row['gender'], axis=1)

    # height와 weight를 정수로 변환
    merged_df['height'] = (merged_df['height'].astype(float)).astype(int)
    merged_df['weight'] = (merged_df['weight'].astype(float)).astype(int)

    # df1_filtered (product_table)에 해당하는 데이터프레임 분리
    df1_final = merged_df[['product_name', 'product_size', 'category', 'platform', 'brand']]
    df1_final.columns = ['product_name', 'size', 'category', 'platform', 'brand']

    # 1. list 타입의 데이터를 문자열로 변환
    df1_final['size'] = df1_final['size'].apply(lambda x: str(x) if isinstance(x, list) else x)

    # 2. 중복된 행 제거
    df1_final.drop_duplicates(inplace=True)

    # 3. 문자열로 변환된 데이터를 다시 list로 변환
    df1_final['size'] = df1_final['size'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

    # df2_filtered (reviews_table)에 해당하는 데이터프레임 분리
    df2_final = merged_df[['product_name', 'review_size', 'height', 'weight', 'gender', 'size_comment']]
    df2_final.columns = ['product_name', 'size', 'height', 'weight', 'gender', 'size_comment']

# culonculon

    # Push processed data back to XCom
    ti.xcom_push(key="processed_product_df", value=df1_final.to_json())
    ti.xcom_push(key="processed_reviews_df", value=df2_final.to_json())


def save_data_to_redshift(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    ti = kwargs["ti"]
    processed_product_df_json = ti.xcom_pull(key="processed_product_df")
    processed_reviews_df_json = ti.xcom_pull(key="processed_reviews_df")

    processed_product_df = pd.read_json(processed_product_df_json)
    processed_reviews_df = pd.read_json(processed_reviews_df_json)

    print(processed_product_df)
    print(processed_reviews_df)
    # # Ensure tables exist and use schema and table names as specified
    # cursor.execute(
    #     """
    #     DROP TABLE IF EXISTS otto."29cm_product" CASCADE;
    #     CREATE TABLE IF NOT EXISTS otto."29cm_product" (
    #         product_name TEXT,
    #         size TEXT,
    #         category TEXT,
    #         platform TEXT,
    #         brand TEXT
    #     );
    #     """
    # )

    # cursor.execute(
    #     """
    #     DROP TABLE IF EXISTS otto."29cm_reviews" CASCADE;
    #     CREATE TABLE IF NOT EXISTS otto."29cm_reviews" (
    #         product_name TEXT,
    #         size TEXT,
    #         height NUMERIC,
    #         weight NUMERIC,
    #         gender TEXT,
    #         size_comment TEXT
    #     );
    #     """
    # )

    # # Insert data into 29cm_product table
    # for _, row in processed_product_df.iterrows():
    #     cursor.execute(
    #         """
    #         INSERT INTO otto."29cm_product" (product_name, size, category, platform, brand)
    #         VALUES (%s, %s, %s, %s, %s)
    #         """,
    #         (
    #             row["product_name"],
    #             json.dumps(row["size"]),
    #             row["category"],
    #             row["platform"],
    #             row["brand"],
    #         ),
    #     )

    # # Insert data into 29cm_reviews table
    # for _, row in processed_reviews_df.iterrows():
    #     cursor.execute(
    #         """
    #         INSERT INTO otto."29cm_reviews" (product_name, size, height, weight, gender, size_comment)
    #         VALUES (%s, %s, %s, %s, %s, %s)
    #         """,
    #         (
    #             row["product_name"],
    #             row["size"],
    #             row["height"],
    #             row["weight"],
    #             row["gender"],
    #             row["size_comment"],
    #         ),
    #     )

    # conn.commit()
    # cursor.close()
    # conn.close()
