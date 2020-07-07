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
    - Fetch data daily with crawler;
    - Save data as-is to S3 (DL);
    - Transform the raw data with spark and save back to S3 in parquet format(stage area);
    - Run DDLs to Redshift;
        - Tables: `staging_game_match`, `dim_champion`, `dim_item`, `dim_summoner`, `fact_game_match`
        - Tables should have appropriate `data types`, `distkeys` and `sortkeys`;
        - Distkeys and Sortkeys strategies: TODO
    - Create a `json_paths` file?
    - COPY data from s3 to stage table `staging_game_match` in S3;
    - Run DMLs to populate dimension and fact tables;
        - For the dim tables use the `truncate-insert` approach;
        - For the fact tables use the `append-only` approach;
    - Run quality checks;
    - Make queries;
