import boto3
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
            # 가장 마지막 파일을 선택
            last_file = files[-1]
            self.log.info(f"Found files in {self.s3_root}. Last file: {last_file}")
        else:
            self.log.info(f"No files found in {self.s3_root}.")
