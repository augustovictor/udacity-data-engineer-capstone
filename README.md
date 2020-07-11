Instructions
To help guide your project, we've broken it down into a series of steps.

- Step 1: Scope the Project and Gather Data
Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step, you’ll:
Identify and gather the data you'll be using for your project (at least two sources and more than 1 million rows).
See Project Resources for ideas of what data you can use.
Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

- Step 2: Explore and Assess the Data
Explore the data to identify data quality issues, like missing values, duplicate data, etc.
Document steps necessary to clean the data

- Step 3: Define the Data Model
Map out the conceptual data model and explain why you chose that model
List the steps necessary to pipeline the data into the chosen data model

- Step 4: Run ETL to Model the Data
Create the data pipelines and the data model
Include a data dictionary
Run data quality checks to ensure the pipeline ran as expected
Integrity constraints on the relational database (e.g., unique key, data type, etc.)
Unit tests for the scripts to ensure they are doing the right thing
Source/count checks to ensure completeness

- Step 5: Complete Project Write Up
What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
Clearly state the rationale for the choice of tools and technologies for the project.
Document the steps of the process.
Propose how often the data should be updated and why.
Post your write-up and final data model in a GitHub repo.
Include a description of how you would approach the problem differently under the following scenarios:
If the data was increased by 100x.
If the pipelines were run on a daily basis by 7am.
If the database needed to be accessed by 100+ people.

[Rubric](https://review.udacity.com/#!/rubrics/2497/view)

## Links
- [Airflow](http://localhost:8080)
- [Riot API key](https://developer.riotgames.com/)

## Scope
The goal of the project is to store available dota2 matches details on a DW so analytical queries can be performed;
At a latter moment, run ML scripts on them to take informed decisions in near real time during the game;

## Data assessment
RATE LIMITS
20 requests every 1 seconds(s)
100 requests every 2 minutes(s)
Note that rate limits are enforced per routing value (e.g., na1, euw1, americas).


To install poetry with specific version:
- Make sure you have no activated virtual env (deactivate if you do);
- Set current python version to the desired one you want poetry to use;
- Run `poetry init` and set the python version to the desired one;
- Copy the result of `which python`;
- Run `poetry env use <path_to_python>`;
- Run `poetry run jupyter notebook`, for example;

## References
- https://towardsdatascience.com/how-to-use-riot-api-with-python-b93be82dbbd6

### TODO:
- Airflow:
    - [] Fetch data daily with crawler;
    - [OK] Save data as-is to S3 (DL);
    - [OK] Transform the raw data with spark and save back to S3 in parquet format(stage area);
    - Run DDLs to Redshift;
        - [OK] Tables: `staging_game_match`, `dim_champion`, `dim_item`, `dim_summoner`, `fact_game_match`
        - [] Tables should have appropriate `data types`, `distkeys` and `sortkeys`;
        - [] Distkeys and Sortkeys strategies: TODO
    - [X] Create a `json_paths` file? Not necessary since we're not importing json data;
    - [OK] COPY data from s3 to stage table `staging_game_match` in S3;
    - Run DMLs to populate dimension and fact tables;
        - [] For the dim tables use the `truncate-insert` approach;
        - [OK] For the fact tables use the `append-only` approach;
    - [] Run quality checks;
    - [] Make adhoc queries;


## Rubric
- [OK] Scoping the Project
- Addressing Other Scenarios
    - [] The data was increased by 100x.
    - [OK] The pipelines would be run on a daily basis by 7 am every day
    - [] The database needed to be accessed by 100+ people.
- [OK] Defending Decisions
- [OK] Project code is clean and modular
- [] Quality Checks
- Data Model
    - [OK] The ETL processes result in the data model outlined in the write-up.
    - [OK] A data dictionary for the final data model is included.
    - [OK] The data model is appropriate for the identified purpose.
- Datasets
    - [OK] At least 2 data sources
    - [OK] More than 1 million lines of data.
    - [OK] At least two data sources/formats (csv, api, json)

### Redshift
- To get data copied from s3 to redshift (at least when using parquet) you should modify the cluster to add an IAM ROLE through `actions` > `Manage IAM roles` ;
    - Add a role that has permission to TODO;
```
  -----------------------------------------------
  error:  User arn:aws:redshift:us-west-2:782148276433:dbuser:sparkify-dw/sparkifyuser is not authorized to assume IAM Role aws_iam_role=arn:aws:iam::782148276433:role/sparkify.dw.role
  code:      8001
  context:   IAM Role=aws_iam_role=arn:aws:iam::782148276433:role/sparkify.dw.role
  query:     913
  location:  xen_aws_credentials_mgr.cpp:321
  process:   padbmaster [pid=25930]
  -----------------------------------------------
```
- `statement_timeout`:
    - Run `show all;` command in redshift query editor;
    - `statement_timeout` = 0 means there is not timeout;
- When importing data from S3 to Redshift tables an error was occurring. Unsuccessfuull investigation steps:
    - Shorten parquet parts size;
    - Remove `repartition` statement from spark code before saving to s3 in parquet format;
    - Save only a subset of the fields;
    - Solution:
        - Save data as json in S3 then run `COPY` command on that; I realized that `ts` field was resulting in wrong `TIMESTAMP` result;
            - Some other fields had wrong specified sizes. Eg: `game_type` field was VARCHAR(10). After increasing wrong max sizes, all worked fine;
        - Further considerations:
            - Really bad feedback on `COPY` command error. Could only see what was going wrong after changing the output format from `parquet` to `json`, and verify errors in `stl_load_errors` redshift table;
            - Would it be a good idea to import a sampling data first?
                - That would not resolve the root cause since the sampling could have only valid data;
            - Would it be worth importing the whole data as `json` then after all works well change implementation and save data as `parquet`?
                - This approach does not scale and takes a lot more time to import the data;
### Spark
- `pyspark` and `spark` need to match versions;
- Use findspark module;
- [SparkUI](http://localhost:4040/jobs)

### Running the project:
1. Start redshift cluster: `make redshift-resume`
1. Start emr cluster: `make emr-cluster-up`
1. Terminate emr cluster: `make emr-cluster-down cluster-id=<CLUSTER_ID>`
1. Pause redshift cluster: `make redshift-pause`

#### References

- [Optimizing Performance](https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html)
- [Hadoop Scalability and Performance Testing in Heterogeneous Clusters](https://www.researchgate.net/publication/291356207_Hadoop_Scalability_and_Performance_Testing_in_Heterogeneous_Clusters)
- [Scaling Uber’s Apache Hadoop Distributed File System for Growth](https://eng.uber.com/scaling-hdfs)
- [Data dictionary](https://www.tutorialspoint.com/What-is-Data-Dictionary)
- [Building An Analytics Data Pipeline In Python](https://www.dataquest.io/blog/data-pipelines-tutorial)
- [Redshift data types](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)
- [Redshift numeric types](https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html)
- [Pyspark extension types](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-types.html)
- [Remotely submit emr spark job](https://aws.amazon.com/premiumsupport/knowledge-center/emr-submit-spark-job-remote-cluster/)
- [Terminate emr cluster](https://docs.aws.amazon.com/emr/latest/ManagementGuide/UsingEMR_TerminateJobFlow.html)
- [Airflow ssh operator](https://airflow.readthedocs.io/en/stable/howto/connection/ssh.html)
- [Authorizing COPY, UNLOAD, and CREATE EXTERNAL SCHEMA operations using IAM roles](https://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html)
- [Redshift server configuration](https://docs.aws.amazon.com/redshift/latest/dg/t_Modifying_the_default_settings.html)
    - [Redshift statement timeout](https://docs.aws.amazon.com/redshift/latest/dg/r_statement_timeout.html)
- [Airflow repo](https://github.com/puckel/docker-airflow)
