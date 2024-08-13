import boto3
import pandas as pd  # pandas 모듈 이름 수정
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3ListOperator(BaseOperator):
    """
    S3 버킷의 파일 목록을 출력하는 사용자 정의 오퍼레이터.
    """

    @apply_defaults
    def __init__(self, aws_conn_id, bucket_name, s3_root, *args, **kwargs):
        super(S3ListOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.s3_root = s3_root
        
    def execute(self, context):
        # S3Hook을 사용하여 S3에 연결
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        # 주어진 s3_root 경로의 파일 목록을 가져옴
        files = s3_hook.list_keys(bucket_name=self.bucket_name, prefix=self.s3_root)
        
        if files:
            for i in range(len(files)):
                print(i, '번 째 파일 : ',files[i])
            
            # 가장 마지막 파일을 선택
            last_file = files[-1]
            self.log.info(f"Found files in {self.s3_root}. Last file: {last_file}")
        else:
            self.log.info(f"No files found in {self.s3_root}.")


class CrawlingOperator(BaseOperator):
    """
    크롤링 작업을 수행하는 사용자 정의 오퍼레이터.
    """
    @apply_defaults
    def __init__(self, aws_conn_id, bucket_name, reviews_s3_root, products_s3_root, *args, **kwargs):
        super(CrawlingOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.reviews_s3_root = reviews_s3_root
        self.products_s3_root = products_s3_root
        
        # S3 연결
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        
        # 공통 데이터 프레임 컬럼 정의
        self.product_df_col_name = ["product_id", "rank", "product_name", "category", "price", 
                                    "image_url", "description", "color", "size", "platform"]
        
        self.review_df_col_name = ["review_id", "product_name", "color", "size", "height", 
                                "gender", "weight", "top_size", "bottom_size", 
                                "size_comment", "quality_comment", "color_comment", 
                                "thickness_comment", "brightness_comment", "comment"]

    def execute(self, context):
        # 각 크롤링 메서드 호출 시 컬럼 이름 리스트 전달
        musinsa_df = self.crawling_musinsa(self.product_df_col_name, context)
        df_29cm = self.crawling_29cm(self.product_df_col_name, context)
        zigzag_df = self.crawling_zigzag(self.product_df_col_name, context)

    def crawling_musinsa(self, col_name, context):
        print(f"Columns for musinsa: {col_name}")
        # 크롤링 로직 구현
        return pd.DataFrame(columns=col_name)

    def crawling_29cm(self, col_name, context):
        print(f"Columns for 29cm: {col_name}")
        # 크롤링 로직 구현
        return pd.DataFrame(columns=col_name)

    def crawling_zigzag(self, col_name, context):
        print(f"Columns for zigzag: {col_name}")
        # 크롤링 로직 구현
        return pd.DataFrame(columns=col_name)
