from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class EmrOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            *args,
            **kwargs
    ):
        super(EmrOperator, self).__init__(*args, **kwargs)
        self.cluster_id = kwargs['cluster_id']
        self.cluster_dns = kwargs['cluster_dns']

    def execute(self, context):
        pass
