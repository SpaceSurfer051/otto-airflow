import boto3
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class S3ListOperator(BaseOperator):
    """
    S3 버킷의 파일 목록을 출력하는 사용자 정의 오퍼레이터.
    """

    @apply_defaults
    def __init__(self, aws_conn_id, bucket_name, *args, **kwargs):
        super(S3ListOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name

    def execute(self, context):
        # Airflow connection에서 AWS 자격 증명 가져오기
        session = boto3.Session(profile_name=self.aws_conn_id)
        s3 = session.client('s3')

        # S3 버킷의 파일 목록을 가져옴
        objects = s3.list_objects_v2(Bucket=self.bucket_name)
        
        if 'Contents' in objects:
            for obj in objects['Contents']:
                self.log.info(f"Found file: {obj['Key']}")
        else:
            self.log.info("No files found in bucket.")
