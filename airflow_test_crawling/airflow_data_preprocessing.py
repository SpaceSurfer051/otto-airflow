import pandas as pd
from airflow.hooks.S3_hook import S3Hook
from io import StringIO

def test_print():
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    bucket_name = 'papalio-test-bucket'
    key = 'test_otto/products_with_size_color.csv'

    s3_client = s3_hook.get_conn()

    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    
    # 데이터프레임 출력
    print(df)
    # 전체 데이터의 길이 출력
    print(f"Total number of rows: {len(df)}")
    #test_preprocessing(df)
    

#def test_preprocessing(df):
    '''
    product_id,product_name,category,price,image_url,description,size,reviews,size_options,color_options 컬럼 정보
    
    유지 : product_name,category,image_url,description
    
    product_id : 매 전처리마다 순위라기 보다는 열 갯수 느낌으로 Top_{n} , pants_{n}, one_piece_{n} 느낌으로 가기
    
    review : 비어있으면 그냥 비어있는대로 냅두기 (나중에 채울 것)
    
    review 구조 : 
                    review = {
                "weight_height_gender": weight_height_gender,
                "review_id": review_id,
                "top_size": top_size,
                "brightness_comment": brightness_comment,
                "color_comment": color_comment,
                "thickness_comment": thickness_comment,
                "purchased_product_id": purchased_product_id,
                "purchased_size": purchased_size,
                "comment": comment
            }
    
    
    price : 앞 부분 숫자만 가져오기
    
    size : 85~115 단위로 통일하기
    
    size_options과 color_options을 바꾸기
    이후에 size와 color 정보 수정할 예정
    '''


    
    
    #return predf
