"""
Example of ETL implementation using plain Python functions.

Extracts data from Google Bigquery, transforms it and loads it to Sqlite.

The resulting data can be seen by running:

    $ sqlite3 github.db
    sqlite> .schema 20220915
    sqlite> select * from '20220915';
"""
import json

import pandas as pd
import sqlalchemy


def extract():
    """Extract a subset of pull requests data from a source BigQuery table"""
    sql_statement = """
    SELECT payload FROM githubarchive.day.20220914
    WHERE
      repo.name="apache/airflow" AND
      type="PullRequestEvent" AND
      json_extract_scalar(payload, "$.action")="closed"
    """
    engine = sqlalchemy.engine.create_engine("bigquery://astronomer-dag-authoring/temp_astro")
    return engine.execute(sql_statement).fetchall()


def transform(rows):
    """Transform the data, by extracting meaningful information"""
    new_rows = []
    for row in rows:
        payload = json.loads(row[0])
        new_row = {
            "repo": payload["pull_request"]["head"]["repo"]["full_name"],
            "number": payload["number"],
            "action": payload["action"],
            "author": payload["pull_request"]["user"]["login"],
            "commits": payload["pull_request"]["commits"],
            "additions": payload["pull_request"]["additions"],
            "deletions": payload["pull_request"]["deletions"],
            "changed_files": payload["pull_request"]["changed_files"],
            "review_comments": payload["pull_request"]["review_comments"],
            "merged": payload["pull_request"]["merged"],
            "created_at": payload["pull_request"]["created_at"],
            "merged_at": payload["pull_request"]["merged_at"],
            "closed_at": payload["pull_request"]["closed_at"],
        }
        transformed_rows.append(new_row)
    return pd.DataFrame(new_rows)


def enrich(df):
    """Infer new information per row."""
    df = df[(df['merged'] == True) & (df["action"] == "closed")]
    df[["created_at", "closed_at"]] = df[["created_at", "closed_at"]].apply(pd.to_datetime)
    df['days_to_merge'] = (df['closed_at'] - df['created_at']).dt.days
    df['lines_diff'] = df['additions'] - df['deletions']
    return df


def load(pandas_dataframe):
    """Load the data of interest to Sqlite database"""
    db = sqlalchemy.create_engine("sqlite:///github.db")
    pandas_dataframe.to_sql("20220915", db, if_exists="replace")


if __name__ == "__main__":
    rows = extract()
    transformed_rows = transform(rows)
    enriched_rows = enrich(transformed_rows)
    load(enriched_rows)
