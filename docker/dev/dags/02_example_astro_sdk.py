import astro.sql as aql
import pandas as pd
import pendulum
from airflow import DAG
from astro.files import File
from astro.sql.table import Table

DATABASE_URI = "sqlite:///github.db"


@aql.transform
def transform(input_table):
    """Clean the original data, only keeping relevant fields"""
    return """
        SELECT
          json_extract(payload, "$.pull_request.head.repo.full_name") as repo_name,
          json_extract(payload, "$.pull_request.number") as pr_number,
          json_extract(payload, "$.number") as pr_id,
          json_extract(payload, "$.pull_request.additions") as pr_additions,
          json_extract(payload, "$.pull_request.deletions") as pr_deletions,
          json_extract(payload, "$.pull_request.created_at") as created_at,
          json_extract(payload, "$.pull_request.merged_at") as merged_at
        FROM {{input_table}}
        WHERE
          type="PullRequestEvent" AND
          json_extract(payload, "$.action")="closed" AND
          json_extract(payload, "$.pull_request.merged") IS True AND
          json_extract(payload, "$.pull_request.merged_at") IS NOT NULL
    """


@aql.transform
def enrich(input_table):
    """Add additional data which enriches the original data"""
    return """
        SELECT
          repo_name,
          pr_number,
          JULIANDAY(merged_at) - JULIANDAY(created_at) AS days_to_merge,
          pr_additions - pr_deletions AS new_code_lines
        FROM {{input_table}}
    """


@aql.transform
def summarise(input_table):
    """Summarise the metrics per repository"""
    return """
        SELECT
            repo_name,
            AVG(days_to_merge),
            AVG(new_code_lines),
            COUNT(*) as total_prs
        FROM {{input_table}}
        GROUP BY repo_name
    """


@aql.dataframe
def analyse(df: pd.DataFrame):
    """Summarise the metrics per repository"""
    print("\n\nThese projects may need a closer eye:")
    to_review = df.loc[
        (df["avg(days_to_merge)"] >= 7)
        | (df["avg(new_code_lines)"] > 400)
        | (df["total_prs"] == 0)
    ]
    to_review = to_review.sort_values(by=["avg(days_to_merge)"], ascending=False)
    print(to_review)

    print("\n\nThese projects seem well:")
    seem_well = df.loc[
        (df["avg(days_to_merge)"] < 2)
        & (df["avg(new_code_lines)"] < 400)
        & (df["total_prs"] > 0)
    ]
    seem_well = seem_well.sort_values(by=["avg(new_code_lines)"])
    print(seem_well)


input_file = File(
    path="https://storage.googleapis.com/pyconuk-workshop/githubarchive/day/2022/09/01/githubarchive.day.20220901.ndjson"
)

original_pr_table = Table(name="day20220901", conn_id="sqlite_default")

with DAG(
    "02_example_astro_sdk",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    original_table = aql.load_file(input_file, output_table=original_pr_table)
    clean_pr_table = transform(original_table)
    enriched_pr_table = enrich(clean_pr_table)
    summarise_pr_table = summarise(enriched_pr_table)
    analyse(summarise_pr_table)
