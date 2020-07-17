from typing import List


class S3UriFormatter:
    @staticmethod
    def get_s3_path_for(aws_bucket: str, aws_data_keys: List[str]) -> str:
        keys = "/".join(aws_data_keys)
        return f"s3a://{aws_bucket}/{keys}"
