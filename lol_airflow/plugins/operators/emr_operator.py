from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class EmrOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            cluster_dns: str,
            cluster_id: str,
            *args,
            **kwargs
    ):
        super(EmrOperator, self).__init__(*args, **kwargs)
        self.cluster_dns = cluster_dns
        self.cluster_id = cluster_id

    def execute(self, context):
        pass
