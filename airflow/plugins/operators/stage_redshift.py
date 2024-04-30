from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from airflow.contrib.hooks import AwsHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql_stmt = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    IGNOREHEADER {}
    DELIMITER '{}'
    FORMAT AS JSON auto
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 delimiter = ",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        

    def execute(self, context):
        self.log.info('StageToRedshiftOperator start')
        aws_hook = AwsBaseHook(aws_conn_id='aws_credentials', resource_type='*')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql_stmt.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
        )
        redshift.run(formatted_sql)
        





