import requests
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FetchAndStageChampionsExternalData(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            aws_credentials_id: str,
            base_url: str,
            s3_bucket: str,
            s3_key: str,
            *args, **kwargs
    ):
        super(FetchAndStageChampionsExternalData, self).__init__(*args, **kwargs)
        self.base_url = base_url
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_hook = S3Hook(aws_conn_id=aws_credentials_id)

    def execute(self, context):
        url = f"{self.base_url}/champion.json"
        r = requests.get(url=url)

        if r.status_code == 200:
            result = r.json()
            keys = result['data'].keys()

            for i, key_item in enumerate(keys):
                self.logger.info(f"[{i}/{len(keys)}] Fetching data for {key_item}...")
                current_s3_key = f"{self.s3_key}/{key_item}.json"

                self.s3_hook.load_bytes(
                    bytes_data=r.content,
                    bucket_name=self.s3_bucket,
                    key=current_s3_key,
                    replace=True,
                )
        else:
            error_message = f"Error while fetching data from [{url}]." \
                            f"Received status [{r.status_code}] != 200"

            self.logger.info(error_message)
            raise ValueError(error_message)

    def _get_champion(self, champion_identifier: str):
        url=f"{self.base_url}/{champion_identifier}.json"

        r = requests.get(url=url)

        if r.status_code == 200:
            current_s3_key = f"{self.s3_key}/{champion_identifier}.json"
            self.s3_hook.load_bytes(
                bytes_data=r.content,
                bucket_name=self.s3_bucket,
                key=current_s3_key,
                replace=True,
            )
        else:
            error_message = f"Error while fetching data from [{url}]." \
                            f"Received status [{r.status_code}] != 200"
