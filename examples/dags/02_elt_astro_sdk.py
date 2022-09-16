from datetime import datetime

#import numpy as np
import pandas as pd
from airflow import DAG
#from airflow.decorators import task

from astro import sql as aql
from astro.sql.table import Table, Metadata


@aql.transform
def extract_pull_request_events(input_table):
    return """
    SELECT * FROM {{input_table}}
    WHERE repo.name="apache/airflow" and type="PullRequestEvent"
    """


@aql.transform
def transform_pull_requests(input_table):
    return """
    SELECT 
      json_extract_scalar(payload, "$.number") as id,
      json_extract_scalar(payload, "$.action") as action,
      json_extract_scalar(payload, "$.pull_request.user.login") as author,
      json_extract_scalar(payload, "$.pull_request.commits") as commits,
      json_extract_scalar(payload, "$.pull_request.additions") as additions,
      json_extract_scalar(payload, "$.pull_request.deletions") as deletions,
      json_extract_scalar(payload, "$.pull_request.changed_files") as changed_files,
      json_extract_scalar(payload, "$.pull_request.review_comments") as review_comments,
      json_extract_scalar(payload, "$.pull_request.merged") as merged,
      json_extract_scalar(payload, "$.pull_request.created_at") as created_at,
      json_extract_scalar(payload, "$.pull_request.updated_at") as updated_at,
      json_extract_scalar(payload, "$.pull_request.closed_at") as closed_at,
      json_extract_scalar(payload, "$.pull_request.merged_at") as merged_at
    FROM {{input_table}}
    """


@aql.dataframe
def analyse_pull_requests(pull_requests: pd.DataFrame):
    # calculate average number of days a PR took to be merged
    # calculate total prs which were merged
    df = pull_requests
    print(f"Processing a total of {df.shape[0]} pull request events")
    df = df[(df['merged'] == "true") & (df["action"] == "closed")]

    print(f"A total of {df.shape[0]} pull requests which have been merged")

    df[["created_at", "closed_at"]] = df[["created_at", "closed_at"]].apply(pd.to_datetime)
    df['days_to_merge'] = (df['closed_at'] - df['created_at']).dt.days

    print("Analysis of the time that it took to merge a PR:")
    print(df["days_to_merge"].describe())


with DAG(
     "example_02_etl_airflow",
     schedule_interval=None,
     start_date=datetime(2000, 1, 1),
     catchup=False,
) as dag:

    github_events_20220914 = Table(
        name="20220914",
        metadata=Metadata(
            schema="day",
            database="githubarchive"
        ),
        conn_id="google_cloud_default",
    )

    github_pull_requests = extract_pull_request_events(github_events_20220914)
    pull_requests = transform_pull_requests(github_pull_requests)
    analyse_pull_requests(pull_requests=pull_requests)