from typing import Optional

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.macros import ds_format
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Docs
    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_credentials_id: str = "",
                 iam_redshift_role: str = "",
                 target_table: str = "",
                 s3_bucket: Optional[str] = None,
                 s3_key: Optional[str] = None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.iam_redshift_role = iam_redshift_role
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_hook = S3Hook(aws_conn_id=aws_credentials_id)

    def execute(self, context):
        self.log.info("CONTEXT")
        self.log.info(context)
        execution_date = context.get('execution_date').strftime("%Y-%m-%d")

        year = ds_format(execution_date, '%Y-%m-%d', '%Y')
        month = ds_format(execution_date, '%Y-%m-%d', '%m')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # s3_file_path = f"s3://{self.s3_bucket}/{self.s3_key}/{year}/{month}"
        s3_file_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        formatted_s3_copy_command = self.get_formatted_s3_copy_command(
            target_table=self.target_table,
            s3_path=s3_file_path,
            iam_redshift_role=self.iam_redshift_role,
        )

        self.log.info(f"Copying data from s3 '{s3_file_path}' to redshift "
                      f"table '{self.target_table}'...")

        if self.s3_hook.check_for_bucket(bucket_name=self.s3_bucket):
            redshift.run(formatted_s3_copy_command)
        else:
            self.log.info(f"S3 object '{s3_file_path}' does not exist...")

    @staticmethod
    def get_formatted_s3_copy_command(
            target_table: str,
            s3_path:str,
            iam_redshift_role: str,
    ) -> str:
        # result = ("COPY {} FROM '{}' IAM_ROLE '{}' FORMAT AS PARQUET;"
        #           .format(target_table, s3_path, iam_redshift_role))
        result = ("COPY {} FROM '{}' CREDENTIALS '{}' json 'auto' REGION '{}'"
            .format(target_table, s3_path, iam_redshift_role, "us-west-2"))

        return result
