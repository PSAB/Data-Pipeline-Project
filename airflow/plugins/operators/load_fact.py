from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql_script = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_script = sql_script

    def execute(self, context):
        self.log.info('LoadFactOperator start')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        table_insert_script = f"""
        INSERT INTO {self.table} {self.sql_script}
        """
        redshift_hook.run(table_insert_script)
        
