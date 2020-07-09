from airflow.plugins_manager import AirflowPlugin
import operators


class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    
    operators = [
        operators.DataQualityOperator,
        operators.DataQualityValidator,
        operators.LoadDimensionOperator,
        operators.LoadFactOperator,
        operators.StageToRedshiftOperator,
         operators.DdlRedshiftOperator,
         operators.FetchAndStageItemsExternalData,
         operators.FetchAndStageChampionsExternalData,
         operators.FetchAndStageMatchesExternalData,
         operators.EmrOperator,
    ]

    helpers = [

    ]
