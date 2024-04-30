from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 append_insert_type = False,
                 primary_key = "",
                 specified_sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_insert_type = append_insert_type
        self.specified_sql = specified_sql
        self.primary_key = primary_key

    def execute(self, context):
        self.log.info('LoadDimensionOperator start')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        table_insert_script = ""
        if not self.append_insert_type:
            table_insert_script = f"""
            INSERT INTO {self.table} {self.specified_sql}
            """
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")
        else:
            table_insert_script = f"""
            CREATE TEMP TABLE stage_{self.table} (like {self.table});
            INSERT INTO stage_{self.table} {self.specified_sql};
            DELETE FROM {self.table}
            USING stage_{self.table}
            WHERE {self.table}.{self.primary_key} = stage_{self.table}.{self.primary_key};
            INSERT INTO {self.table}
            SELECT * FROM stage_{self.table};
            """
        redshift_hook.run(table_insert_script)
        
            
         
        
        
        
