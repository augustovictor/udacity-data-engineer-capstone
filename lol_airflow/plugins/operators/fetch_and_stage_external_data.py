import requests
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FetchAndStageExternalData(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(FetchAndStageExternalData, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.s3_hook = S3Hook(aws_conn_id=context["aws_credentials_id"])
        self.s3_bucket = context["s3_bucket"]
        self.s3_key = context["s3_key"]
        self.base_url = context["base_url"]

        r = requests.get(url=self.base_url)

        if r.status_code == 200:
            result = r.json()
            keys = result['data'].keys()

            for i, key_item in enumerate(keys):
                self.logger.info(f"[{i}/{len(keys)}] Fetching data for {key_item}...")
                self.s3_key += f"/{key_item}.json"

                self.s3_hook.load_string(
                    string_data=str(r.json()),
                    bucket_name=self.s3_bucket,
                    key=self.s3_key,
                )
        else:
            error_message = f"Error while fetching data from [{self.base_url}]." \
                            f"Received status [{r.status_code}] != 200"

            self.logger.info(error_message)
            raise ValueError(error_message)
