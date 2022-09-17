import tempfile

import pandas as pd
import pendulum
import requests
import sqlalchemy
from airflow.decorators import dag, task
from airflow import DAG


DATABASE_URI = "sqlite:///github.db"


def create_table(table_name, engine):
    """
    Create a SQLite table with the correct schema to process Github archive events.
    """
    metadata = sqlalchemy.MetaData(engine)
    _ = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("type", sqlalchemy.String),
        sqlalchemy.Column("public", sqlalchemy.Boolean),
        sqlalchemy.Column("payload", sqlalchemy.String),
        sqlalchemy.Column("repo", sqlalchemy.String),
        sqlalchemy.Column("actor", sqlalchemy.String),
        sqlalchemy.Column("org", sqlalchemy.String),
        sqlalchemy.Column("created_at", sqlalchemy.String),
        sqlalchemy.Column("other", sqlalchemy.String),
    )
    metadata.create_all()


@task()
def load(file_url, table_name):
    """Load a remote NDJSON file into a local SQlite table"""
    http_response = requests.get(file_url, allow_redirects=True)
    with tempfile.NamedTemporaryFile() as temp_file:
        with open(temp_file.name, "wb") as fp:
            fp.write(http_response.content)
        df = pd.read_json(temp_file.name, lines=True)
        df = df.astype(str)

    engine = sqlalchemy.create_engine(DATABASE_URI)
    create_table(table_name, engine)

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    return table_name


@task()
def transform(input_table, output_table):
    """Clean the original data, only keeping relevant fields"""
    sql_statement = f"""
    CREATE TABLE IF NOT EXISTS {output_table} AS
        SELECT
          json_extract(payload, "$.pull_request.head.repo.full_name") as repo_name,
          json_extract(payload, "$.pull_request.number") as pr_number,
          json_extract(payload, "$.number") as pr_id,
          json_extract(payload, "$.pull_request.additions") as pr_additions,
          json_extract(payload, "$.pull_request.deletions") as pr_deletions,
          json_extract(payload, "$.pull_request.created_at") as created_at,
          json_extract(payload, "$.pull_request.merged_at") as merged_at
        FROM '{input_table}'
        WHERE
          type="PullRequestEvent" AND
          json_extract(payload, "$.action")="closed" AND
          json_extract(payload, "$.pull_request.merged") IS True AND
          json_extract(payload, "$.pull_request.merged_at") IS NOT NULL
    """
    engine = sqlalchemy.engine.create_engine(DATABASE_URI)
    engine.execute(sql_statement)


@task()
def enrich(input_table, output_table):
    """Add additional data which enriches the original data"""
    sql_statement = f"""
    CREATE TABLE IF NOT EXISTS {output_table} AS
        SELECT
          repo_name,
          pr_number,
          JULIANDAY(merged_at) - JULIANDAY(created_at) AS days_to_merge,
          pr_additions - pr_deletions AS new_code_lines
        FROM '{input_table}'
    """
    engine = sqlalchemy.engine.create_engine(DATABASE_URI)
    engine.execute(sql_statement)


@task()
def summarise(input_table, output_table):
    """Summarise the metrics per repository"""
    sql_statement = f"""
    CREATE TABLE IF NOT EXISTS  {output_table} AS
        SELECT
            repo_name,
            AVG(days_to_merge),
            AVG(new_code_lines),
            COUNT(*) as total_prs
        FROM {input_table}
        GROUP BY repo_name
    """
    engine = sqlalchemy.engine.create_engine(DATABASE_URI)
    engine.execute(sql_statement)


@task()
def analyse(input_table):
    """Summarise the metrics per repository"""
    sql_statement = f"SELECT * FROM {input_table}"
    engine = sqlalchemy.engine.create_engine(DATABASE_URI)
    rows = engine.execute(sql_statement).fetchall()
    df = pd.DataFrame(rows)
    print("\n\nThese projects may need a closer eye:")
    to_review = df.loc[
        (df["AVG(days_to_merge)"] >= 7)
        | (df["AVG(new_code_lines)"] > 400)
        | (df["total_prs"] == 0)
    ]
    to_review = to_review.sort_values(by=["AVG(days_to_merge)"], ascending=False)
    print(to_review)

    print("\n\nThese projects seem well:")
    seem_well = df.loc[
        (df["AVG(days_to_merge)"] < 2)
        & (df["AVG(new_code_lines)"] < 400)
        & (df["total_prs"] > 0)
    ]
    seem_well = seem_well.sort_values(by=["AVG(new_code_lines)"])
    print(seem_well)


file_url = "https://storage.googleapis.com/pyconuk-workshop/githubarchive/day/2022/09/01/githubarchive.day.20220901.ndjson"
original_pr_table = "day20220901"
clean_pr_table = "clean"
enriched_pr_table = "enriched"
summarise_pr_table = "summary"

with DAG(
    "00_example_airflow",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    load(file_url, original_pr_table)
    transform(original_pr_table, clean_pr_table)
    enrich(clean_pr_table, enriched_pr_table)
    summarise(enriched_pr_table, summarise_pr_table)
    analyse(summarise_pr_table)