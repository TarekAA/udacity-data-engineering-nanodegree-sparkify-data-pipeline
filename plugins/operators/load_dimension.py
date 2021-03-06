from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """This operator loads data from staging tables into dimension tables.
    Staging tables resides on the same Redshift cluster.

    The operator provides the option to either append data into the dimension table
    or delete all existing data then load the new data.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 append_on_insert=True,
                 sql_stmnt='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_on_insert = append_on_insert
        if self.append_on_insert:
            self.sql_stmnt = "INSERT INTO {table} (" + sql_stmnt + ')'
        else:
            self.sql_stmnt = "DELETE FROM {table}; " + "INSERT INTO {table} (" + sql_stmnt + ')'

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        rendered_sql_stmnt = self.sql_stmnt.format(table=self.table)
        self.log.info(f"Creating dimension table '{self.table}' from staging tables")
        redshift_hook.run(rendered_sql_stmnt)
