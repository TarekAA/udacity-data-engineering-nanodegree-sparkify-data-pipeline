from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.exceptions import AirflowException


class DataQualityOperator(BaseOperator):
    """This operator run Data Quality checks on the pipeline.

    Checks are provided as a Dict[String, Tuple(String,Union[int,float]]
    the key to the dictionary is the table name,
    the value is a tuple containing the SQL statement and the expected value for comparision.

    Example:
        {
        'staging_events': ("SELECT COUNT(*) FROM staging_events", 8056),
        'staging_songs': ("SELECT COUNT(*) FROM staging_songs", 14896),
        }

    All queries must return a single numeric value like count, average, etc.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dq_table_check_dct={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_table_check_dct = dq_table_check_dct

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        failed_checks = 0
        total_tests = len(self.dq_table_check_dct)
        # iterate checks provided in dictionary checks
        for table, sql_stmnt_test_pair in self.dq_table_check_dct.items():
            sql_stmnt = sql_stmnt_test_pair[0]
            expected_results = sql_stmnt_test_pair[1]
            # execute the query check
            records = redshift_hook.get_records(sql_stmnt)
            # compare provided expected results to actual results, count failed checks
            if records[0][0] != expected_results:
                failed_checks += 1
                self.log.info(
                    f"Data Quality check failed, Expected results:"
                    f" '{expected_results}', Actual Results: '{records[0][0]}'")
        # if any check failed raise an airflow error
        if failed_checks != 0:
            raise AirflowException(
                f"Data Quality check failed: failed_checks/total_tests {failed_checks}/{total_tests}")

        else:  # failed checks == 0
            self.log.info(
                f"All Data Quality checks succeeded: succeeded_checks/total_tests"
                f" {total_tests - failed_checks}/{total_tests}")
