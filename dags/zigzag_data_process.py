from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine, text
import pandas as pd
import re
import random
import json
import pandas as pd
import re
import numpy as np
import random



default_args = {
    "owner": "SB",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "zigzag_data_process",
    default_args=default_args,
    description="Redshift 데이터 처리 후 결과를 다시 Redshift에 저장하는 DAG",
    schedule_interval=None, 
    tags = ["zigzag"]
)


def fetch_data_from_redshift(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    conn = redshift_hook.get_conn()
    sql_product = """
        SELECT product_name, size, category, platform, brand
        FROM otto.product_table
        WHERE platform = 'zigzag';
    """
    sql_reviews = """
        SELECT product_name, size, height, weight, gender, size_comment
        FROM otto.reviews
        WHERE product_name IN (
            SELECT product_name
            FROM otto.product_table
            WHERE platform = 'zigzag'
        );
    """

    product_df = pd.read_sql(sql_product, conn)
    reviews_df = pd.read_sql(sql_reviews, conn)

    # Push DataFrames to XCom
    ti = kwargs["ti"]
    ti.xcom_push(key="product_df", value=product_df.to_json())
    ti.xcom_push(key="reviews_df", value=reviews_df.to_json())

def process(**kwargs):
    ti = kwargs["ti"]
    product_df_json = ti.xcom_pull(key="product_df")
    reviews_df_json = ti.xcom_pull(key="reviews_df")

    p_df = pd.read_json(product_df_json)
    r_df = pd.read_json(reviews_df_json)
    
    #product logic
    p_df['size'] = p_df['size'].apply(remove_korean)
    p_df['size'] = p_df['size'].apply(remove_parentheses_content)
    p_df['size'] = p_df['size'].apply(preprocess_size)
    p_df['size'] = p_df['size'].apply(next_process)
    p_df['size'] = p_df['size'].apply(next_process2)
    p_df['size'] = p_df['size'].apply(next_process3)
    p_df['size'] = p_df['size'].apply(free_to_f)
    p_df['size'] = p_df['size'].replace('', np.nan)
    p_df['size'] = p_df.apply(fill_na_with_review, axis=1, review_df=r_df)

    p_df['size'] = p_df['size'].apply(remove_korean)
    p_df['size'] = p_df['size'].apply(remove_parentheses_content)
    p_df['size'] = p_df['size'].apply(preprocess_size)
    p_df['size'] = p_df['size'].apply(next_process)
    p_df['size'] = p_df['size'].apply(next_process2)
    p_df['size'] = p_df['size'].apply(next_process3)
    p_df['size'] = p_df['size'].apply(free_to_f)
    p_df['size'] = p_df['size'].apply(remove_s_from_string)
    p_df['size'] = p_df['size'].apply(remove_leading_commas)
    p_df['size'] = p_df['size'].replace('', np.nan)

    p_df['size'] = p_df['size'].apply(replace_f_with_sizes)
    p_df['size'] = p_df['size'].apply(remove_quotes)
    p_df['size'] = p_df['size'].apply(delete_space)

    #review logic
    r_df['size'] = r_df['size'].apply(remove_korean)
    r_df['size'] = r_df['size'].apply(remove_parentheses_content)
    r_df['size'] = r_df['size'].apply(preprocess_size)
    r_df['size'] = r_df['size'].apply(next_process)
    r_df['size'] = r_df['size'].apply(next_process2)
    r_df['size'] = r_df['size'].apply(next_process3)
    r_df['size'] = r_df['size'].apply(free_to_f)
    r_df['size'] = r_df['size'].replace('', np.nan)

    r_df['size'] = r_df['size'].replace(to_replace=r'ONE.*', value='', regex=True)

    r_df['size'] = r_df['size'].apply(review_preprocess)
    r_df['size'] = r_df['size'].apply(extract_first_letter)

    r_df['size'] = r_df['size'].apply(separate_size_color)
    r_df['size'] = r_df['size'].apply(replace_numbers_with_nan)

    r_df['size'] = r_df['size'].apply(f_to_null)
    r_df['size'] = r_df['size'].replace('', np.nan)
    first_index = p_df.index[0]
    unique_sizes = p_df.loc[first_index, 'size'].split(',')
    r_df['size'] = r_df.apply(fill_missing_size, axis=1, unique_sizes=unique_sizes)

    r_df['gender'] = r_df['gender'].apply(gender_gen)

    r_df["height"] = r_df.apply(
        lambda row: generate_random_value(row["size"], row["gender"], "height") 
        if pd.isna(row["height"]) or row["height"] == '' else row["height"], 
        axis=1
    )

    r_df["weight"] = r_df.apply(
        lambda row: generate_random_value(row["size"], row["gender"], "weight") 
        if pd.isna(row["weight"]) or row["weight"] == '' else row["weight"], 
        axis=1
        )

    r_df['weight'] = r_df['weight'].replace(to_replace=r'kg', value='', regex=True)
    r_df['height'] = r_df['height'].replace(to_replace=r'cm', value='', regex=True)

    r_df['size_comment'] = r_df['size_comment'].apply(size_change)
    r_df['height'] = pd.to_numeric(r_df['height'], errors='coerce')
    r_df['weight'] = pd.to_numeric(r_df['weight'], errors='coerce')

    
    
    
'''
def process_data(**kwargs):
    ti = kwargs["ti"]
    product_df_json = ti.xcom_pull(key="product_df")
    reviews_df_json = ti.xcom_pull(key="reviews_df")

    p_df = pd.read_json(product_df_json)
    r_df = pd.read_json(reviews_df_json)

    def clean_size_column(size_str):
        size_list_upper = ["XXS", "XS", "S", "M", "L", "XL", "XXL"]
        if isinstance(size_str, str):
            if re.match(r"[가-힣\s]+$", size_str) or "상세" in size_str:
                return "none"
            if (
                size_str.lower() in ["free", "f", "one size"]
                or "chest" in size_str.lower()
            ):
                return ["F"]
            if "~" in size_str:
                pattern = re.compile(r"\b(?:" + "|".join(size_list_upper) + r")\b")
                found_sizes = pattern.findall(size_str.upper())
                if found_sizes:
                    start_size = found_sizes[0]
                    end_size = found_sizes[-1]
                    if start_size in size_list_upper and end_size in size_list_upper:
                        start_index = size_list_upper.index(start_size)
                        end_index = size_list_upper.index(end_size)
                        return size_list_upper[start_index : end_index + 1]
            if "," in size_str:
                size_str = size_str.split(",")
            elif "/" in size_str:
                size_str = size_str.split("/")
            else:
                size_str = ["F"]
        if isinstance(size_str, list):
            cleaned_sizes = []
            for s in size_str:
                s = re.sub(r"\s*\(.*?\)\s*", "", s).strip()
                s = re.split(r"\s+", s, maxsplit=1)[0].strip()
                match = re.search(r"(S|M|L|F)", s)
                if match:
                    s = s[: match.end()].strip()
                cleaned_sizes.append(s)
            size_str = list(dict.fromkeys(cleaned_sizes))
        return size_str

    def select_last_smlf(size_str):
        size_patterns = [
            "3XS",
            "2XS",
            "XXS",
            "XS",
            "S",
            "M",
            "L",
            "XL",
            "XXL",
            "2XL",
            "3XL",
            "F",
        ]
        pattern = re.compile("|".join(size_patterns), re.IGNORECASE)
        size_strip = re.sub(r"\s*\(.*?\)\s*", "", size_str).strip()
        matches = pattern.findall(size_strip)
        if matches:
            size_str = matches[-1].upper()
        else:
            size_str = size_strip
        return size_str

    def convert_size_string_to_list(size_str):
        if isinstance(size_str, str):
            try:
                size_list = eval(size_str)
                if isinstance(size_list, list):
                    return size_list
            except:
                pass
        return []

    size_ranges = {
        "XXXS": {
            "남성": {"height": (140, 150), "weight": (35, 45)},
            "여성": {"height": (130, 140), "weight": (30, 40)},
        },
        "3XS": {
            "남성": {"height": (140, 150), "weight": (35, 45)},
            "여성": {"height": (130, 140), "weight": (30, 40)},
        },
        "XXS": {
            "남성": {"height": (150, 160), "weight": (45, 55)},
            "여성": {"height": (140, 150), "weight": (40, 50)},
        },
        "2XS": {
            "남성": {"height": (150, 160), "weight": (45, 55)},
            "여성": {"height": (140, 150), "weight": (40, 50)},
        },
        "XS": {
            "남성": {"height": (160, 165), "weight": (50, 60)},
            "여성": {"height": (150, 155), "weight": (45, 55)},
        },
        "0": {
            "남성": {"height": (160, 165), "weight": (50, 60)},
            "여성": {"height": (150, 155), "weight": (45, 55)},
        },
        "S": {
            "남성": {"height": (165, 170), "weight": (55, 65)},
            "여성": {"height": (155, 160), "weight": (50, 60)},
        },
        "0.5": {
            "남성": {"height": (165, 170), "weight": (55, 65)},
            "여성": {"height": (155, 160), "weight": (50, 60)},
        },
        "M": {
            "남성": {"height": (170, 175), "weight": (60, 70)},
            "여성": {"height": (160, 165), "weight": (55, 65)},
        },
        "1": {
            "남성": {"height": (170, 175), "weight": (60, 70)},
            "여성": {"height": (160, 165), "weight": (55, 65)},
        },
        "L": {
            "남성": {"height": (175, 180), "weight": (70, 80)},
            "여성": {"height": (165, 170), "weight": (60, 70)},
        },
        "1.5": {
            "남성": {"height": (175, 180), "weight": (70, 80)},
            "여성": {"height": (165, 170), "weight": (60, 70)},
        },
        "XL": {
            "남성": {"height": (180, 185), "weight": (80, 90)},
            "여성": {"height": (170, 175), "weight": (70, 80)},
        },
        "2": {
            "남성": {"height": (180, 185), "weight": (80, 90)},
            "여성": {"height": (170, 175), "weight": (70, 80)},
        },
        "XXL": {
            "남성": {"height": (185, 190), "weight": (90, 100)},
            "여성": {"height": (175, 180), "weight": (80, 90)},
        },
        "2XL": {
            "남성": {"height": (185, 190), "weight": (90, 100)},
            "여성": {"height": (175, 180), "weight": (80, 90)},
        },
        "XXXL": {
            "남성": {"height": (190, 200), "weight": (100, 110)},
            "여성": {"height": (180, 190), "weight": (90, 100)},
        },
        "3XL": {
            "남성": {"height": (190, 200), "weight": (100, 110)},
            "여성": {"height": (180, 190), "weight": (90, 100)},
        },
        "F": {
            "남성": {"height": (165, 185), "weight": (55, 85)},
            "여성": {"height": (155, 175), "weight": (50, 75)},
        },
    }

    def generate_random_value(size, gender, attribute):
        if size in size_ranges and gender in size_ranges[size]:
            min_val, max_val = size_ranges[size][gender][attribute]
        else:
            min_val, max_val = size_ranges["F"][gender][attribute]
        return round(random.uniform(min_val, max_val))

    product_df["size"] = product_df["size"].apply(clean_size_column)
    reviews_df["size"] = reviews_df["size"].apply(select_last_smlf)
    # Update height if it is "none"
    reviews_df.loc[reviews_df["height"] == "none", "height"] = reviews_df.apply(
        lambda row: (
            generate_random_value(row["size"], row["gender"], "height")
            if row["height"] == "none"
            else row["height"]
        ),
        axis=1,
    )

    # Update weight if it is "none"
    reviews_df.loc[reviews_df["weight"] == "none", "weight"] = reviews_df.apply(
        lambda row: (
            generate_random_value(row["size"], row["gender"], "weight")
            if row["weight"] == "none"
            else row["weight"]
        ),
        axis=1,
    )

    processed_product_df = product_df[
        ["product_name", "size", "category", "platform", "brand"]
    ]
    processed_reviews_df = reviews_df[
        ["product_name", "size", "height", "weight", "gender", "size_comment"]
    ]

    ti = kwargs["ti"]
    ti.xcom_push(key="processed_product_df", value=processed_product_df.to_json())
    ti.xcom_push(key="processed_reviews_df", value=processed_reviews_df.to_json())
'''

def save_data_to_redshift(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    ti = kwargs["ti"]
    processed_product_df_json = ti.xcom_pull(key="processed_product_df")
    processed_reviews_df_json = ti.xcom_pull(key="processed_reviews_df")

    processed_product_df = pd.read_json(processed_product_df_json)
    processed_reviews_df = pd.read_json(processed_reviews_df_json)

    # Ensure tables exist and use schema and table names as specified
    cursor.execute(
        """
    DROP TABLE IF EXISTS otto."zigzag_product" CASCADE;
    CREATE TABLE IF NOT EXISTS otto."zigzag_product" (
        product_name TEXT,
        size TEXT,
        category TEXT,
        platform TEXT,
        brand TEXT
    );
    """
    )

    cursor.execute(
        """
    DROP TABLE IF EXISTS otto."zigzag_reviews" CASCADE;
    CREATE TABLE IF NOT EXISTS otto."zigzag_reviews" (
        product_name TEXT,
        size TEXT,
        height NUMERIC,
        weight NUMERIC,
        gender TEXT,
        size_comment TEXT
    );
    """
    )

    # Insert data into 29cm_product table
    for _, row in processed_product_df.iterrows():
        cursor.execute(
            """
            INSERT INTO otto."zigzag_product" (product_name, size, category, platform, brand)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                row["product_name"],
                ",".join(row["size"]) if isinstance(row["size"], list) else row["size"],
                row["category"],
                row["platform"],
                row["brand"],
            ),
        )

    # Insert data into 29cm_reviews table
    for _, row in processed_reviews_df.iterrows():
        cursor.execute(
            """
            INSERT INTO otto."zigzag_reviews" (product_name, size, height, weight, gender, size_comment)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row["product_name"],
                row["size"],
                row["height"],
                row["weight"],
                row["gender"],
                row["size_comment"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


save_task = PythonOperator(
    task_id="save_data",
    python_callable=save_data_to_redshift,
    provide_context=True,
    dag=dag,
)


fetch_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_data_from_redshift,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id="process",
    python_callable=process,
    provide_context=True,
    dag=dag,
)



fetch_task  >> process_task >> save_task
#process_task >> save_task


def remove_korean(text):
    if pd.isna(text):
        return text
    # 정규 표현식을 사용하여 한글 제거
    return re.sub(r'[ㄱ-ㅎㅏ-ㅣ가-힣]', '', text)

def remove_parentheses_content(text):
    if pd.isna(text):
        return text
    # 정규 표현식을 사용하여 괄호와 그 안의 내용 제거
    return re.sub(r'\([^)]*\)', '', text)

def preprocess_size(text):
    if pd.isna(text):
        return text
    # 모든 문자열을 대문자로 변환
    text = text.upper()
    # 'VER'과 '/'를 제거
    text = text.replace('VER', '').replace('/', '').replace('~', '').replace('[', '').replace(']', '')
    text = text.replace("'", '').replace(' ',',').replace(',,',',').replace('"', '')
    #unique_values = ' '.join(sorted(set(text.split())))
    return text

def next_process(text):
    if pd.isna(text):
        return text
    text = text.replace("'", '').replace(' ',',').replace(',,',',').replace('"', '')
    return text

def next_process2(text):
    if pd.isna(text):
        return text
    if text and text[-1] == ',':
        text = text[:-1]
    return text

def sort_key(size):
    sort_order = ['XXXS','XXS','XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL']
    try:
        return sort_order.index(size)
    except ValueError:
        # 만약 size가 sort_order에 없다면, 가장 마지막에 오도록 처리
        return len(sort_order)

def next_process3(text):
    if pd.isna(text):
        return text
    size_list = text.split(',')
    unique_size_list = list(dict.fromkeys(size_list))
    sorted_unique_size_list = sorted(unique_size_list, key=sort_key)
    
    return ','.join(sorted_unique_size_list) 

def free_to_f(text):
    if pd.isna(text):
        return text
    text = text.replace("FREE", 'F')
    return text

def review_preprocess(text):
    if pd.isna(text):
        return text
    text = text.replace(",", '').replace(".",'').replace("CM",'').replace("-",'')
    if text  == '':
        text = np.nan
    return text

def extract_first_letter(size):
    if size in ['SMALL', 'MEDIUM', 'LARGE']:
        return size[0]
    return size

def review_preprocess(text):
    if pd.isna(text):
        return text
    text = text.replace(",", '').replace(".",'').replace("CM",'').replace("-",'')
    if text  == '':
        text = np.nan
    return text

colors = ['BROWN', 'RED', 'BLUE', 'GREEN', 'YELLOW', 'BLACK', 'WHITE','GRAY']
def separate_size_color(value):
    if value in colors:
        return np.nan
    return value

def replace_numbers_with_nan(value):
    try:
        # 값을 float으로 변환 시도
        float_value = float(value)
        return np.nan
    except ValueError:
        # 변환에 실패하면 (즉, 숫자가 아니면) 원래 값을 반환
        return value
    
def review_preprocess(text):
    if pd.isna(text):
        return text
    text = text.replace(",", '').replace(".",'').replace("CM",'').replace("-",'')
    if text  == '':
        text = np.nan
    return text

def extract_first_letter(size):
    if size in ['SMALL', 'MEDIUM', 'LARGE']:
        return size[0]
    return size

colors = ['BROWN', 'RED', 'BLUE', 'GREEN', 'YELLOW', 'BLACK', 'WHITE','GRAY']
def separate_size_color(value):
    if value in colors:
        return np.nan
    return value

def replace_numbers_with_nan(value):
    try:
        # 값을 float으로 변환 시도
        float_value = float(value)
        return np.nan
    except ValueError:
        # 변환에 실패하면 (즉, 숫자가 아니면) 원래 값을 반환
        return value
    
def f_to_null(text):
    if pd.isna(text):
        return text
    text = text.replace("F", '')
    return text

def fill_missing_size(row, unique_sizes):
    if pd.isna(row['size']) or row['size'] == '':
        random_size = np.random.choice(unique_sizes)
        return random_size
    return row['size']

def gender_gen(text):
    if pd.isna(text):
        return text
    text = text.replace("none", '여성')
    return text

size_ranges = {
        "XXXS": {
            "남성": {"height": (140, 150), "weight": (35, 45)},
            "여성": {"height": (130, 140), "weight": (30, 40)},
        },
        "3XS": {
            "남성": {"height": (140, 150), "weight": (35, 45)},
            "여성": {"height": (130, 140), "weight": (30, 40)},
        },
        "XXS": {
            "남성": {"height": (150, 160), "weight": (45, 55)},
            "여성": {"height": (140, 150), "weight": (40, 50)},
        },
        "2XS": {
            "남성": {"height": (150, 160), "weight": (45, 55)},
            "여성": {"height": (140, 150), "weight": (40, 50)},
        },
        "XS": {
            "남성": {"height": (160, 165), "weight": (50, 60)},
            "여성": {"height": (150, 155), "weight": (45, 55)},
        },
        "0": {
            "남성": {"height": (160, 165), "weight": (50, 60)},
            "여성": {"height": (150, 155), "weight": (45, 55)},
        },
        "S": {
            "남성": {"height": (165, 170), "weight": (55, 65)},
            "여성": {"height": (155, 160), "weight": (50, 60)},
        },
        "0.5": {
            "남성": {"height": (165, 170), "weight": (55, 65)},
            "여성": {"height": (155, 160), "weight": (50, 60)},
        },
        "M": {
            "남성": {"height": (170, 175), "weight": (60, 70)},
            "여성": {"height": (160, 165), "weight": (55, 65)},
        },
        "1": {
            "남성": {"height": (170, 175), "weight": (60, 70)},
            "여성": {"height": (160, 165), "weight": (55, 65)},
        },
        "L": {
            "남성": {"height": (175, 180), "weight": (70, 80)},
            "여성": {"height": (165, 170), "weight": (60, 70)},
        },
        "1.5": {
            "남성": {"height": (175, 180), "weight": (70, 80)},
            "여성": {"height": (165, 170), "weight": (60, 70)},
        },
        "XL": {
            "남성": {"height": (180, 185), "weight": (80, 90)},
            "여성": {"height": (170, 175), "weight": (70, 80)},
        },
        "2": {
            "남성": {"height": (180, 185), "weight": (80, 90)},
            "여성": {"height": (170, 175), "weight": (70, 80)},
        },
        "XXL": {
            "남성": {"height": (185, 190), "weight": (90, 100)},
            "여성": {"height": (175, 180), "weight": (80, 90)},
        },
        "2XL": {
            "남성": {"height": (185, 190), "weight": (90, 100)},
            "여성": {"height": (175, 180), "weight": (80, 90)},
        },
        "XXXL": {
            "남성": {"height": (190, 200), "weight": (100, 110)},
            "여성": {"height": (180, 190), "weight": (90, 100)},
        },
        "3XL": {
            "남성": {"height": (190, 200), "weight": (100, 110)},
            "여성": {"height": (180, 190), "weight": (90, 100)},
        },
        "F": {
            "남성": {"height": (165, 185), "weight": (55, 85)},
            "여성": {"height": (155, 175), "weight": (50, 75)},
        },
    }

def generate_random_value(size, gender, attribute):
    if size in size_ranges and gender in size_ranges[size]:
        min_val, max_val = size_ranges[size][gender][attribute]
    else:
        min_val, max_val = size_ranges["F"][gender][attribute]
    return round(random.uniform(min_val, max_val))

def fill_missing_values(row, attribute):
    if pd.isna(row['height']) or row['height'] or pd.isna(row['weight']) or row['weight'] == '':
        row['size'] = generate_random_value('F', row['gender'], attribute)
    return row

def size_change(text):
    if pd.isna(text):
        return text
    if text == "정사이즈예요":
        return '0'
    elif text == "생각보다 커요":
        return '1'
    elif text == "작아요":
        return '-1'
    else:
        return text
    
#unique_sizes = p_df.loc[0, 'size'].split(', ')

def get_review_sizes(product_name, review_df):
    sizes = review_df.loc[review_df['product_name'] == product_name, 'size']
    unique_sizes = sizes.dropna().unique()
    combined_sizes = ','.join(map(str, unique_sizes))
    return combined_sizes

# size가 NaN인 경우 리뷰 데이터프레임의 값을 사용하여 채우는 함수
def fill_na_with_review(row, review_df):
    if pd.isna(row['size']):
        review_sizes = get_review_sizes(row['product_name'], review_df)
        return review_sizes
    return row['size']

def remove_s_from_string(s):
    if isinstance(s, str):
        s = s.replace('ONE', '')
        s = s.replace('SIZE', '')
        
    return s

def remove_leading_commas(s):
    if isinstance(s, str):
        while s.startswith(','):
            s = s[1:]
    return s

def replace_f_with_sizes(s):
    if s == 'F' or s  ==  "" or pd.isna(s):
        return 'S, M, L, XL'
    s = s.replace('"', '').replace("'", "").strip()
    return s

def remove_quotes(s):
    if isinstance(s, str):
        s = s.replace('"', '').replace("'", "").strip()
        
    return s

def delete_space(text):
    if pd.isna(text):
        return text
    text = text.replace(" ", '')
    if text  == '':
        text = np.nan
    return text