from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    """This operator moves data from s3 to Redshift in staging tables.
    Data is considered to be in JSON format.
    The operator uses a copy command with the possibility to provide a specific json_path
    to infer the schema or 'auto' otherwise.

    Postgres hook is used to run the query against Redshift.
    """

    ui_color = '#358140'

    copy_sql_stmnt_template = """
COPY staging_{table}
FROM '{s3_path}'
ACCESS_KEY_ID '{access_key}'
SECRET_ACCESS_KEY '{secret_key}'
JSON '{json_path}'
TRUNCATECOLUMNS
REGION '{region}'
"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_conn_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_path='auto',
                 region='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_conn_id)
        aws_credentials = aws_hook.get_credentials()
        rendered_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'

        rendered_copy_sql_stmnt = StageToRedshiftOperator.copy_sql_stmnt_template.format(
            table=self.table,
            s3_path=s3_path,
            access_key=aws_credentials.access_key,
            secret_key=aws_credentials.secret_key,
            json_path=self.json_path,
            region=self.region)
        self.log.info(f"Staging data from s3 to redshift for table='{self.table}'")
        redshift_hook.run(rendered_copy_sql_stmnt)






