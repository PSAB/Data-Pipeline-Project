from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 query = "",
                 ideal_result = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.ideal_result = ideal_result

    def execute(self, context):
        self.log.info('DataQualityOperator start')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        records = redshift_hook.get_records(self.query)
        if records[0][0] != self.ideal_result:
            raise ValueError("FAIL: Data Quality Check")
        else:
            self.log.info("PASS: Data Quality Check")