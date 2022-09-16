"""
Example of ETL implementation using plain Python functions.

Extracts data from Google Bigquery, transforms it and loads it to Sqlite.

The resulting data can be seen by running:

    $ sqlite3 github.db
    sqlite> .schema 20220915
    sqlite> select * from '20220915';
"""
import pandas as pd
import requests
import sqlalchemy


def extract():
    """Extract a subset of pull requests data from a parquet file available remotely"""
    timestamp = "20220915"
    url = "https://storage.googleapis.com/pyconuk-workshop/github/pull-requests/2022/09/15/000000000000.parquet"
    local_filepath = f"{timestamp}.parquet"
    r = requests.get(url, allow_redirects=True)
    open(local_filepath, "wb").write(r.content)
    df = pd.read_parquet(local_filepath, engine="fastparquet")
    db = sqlalchemy.create_engine("sqlite:///github.db")
    df.to_sql(timestamp, db, if_exists="replace")
    return timestamp


def transform(input_table):
    """Transform the data, by extracting meaningful information"""
    sql_statement = f"""
    SELECT 
      json_extract(payload, "$.pull_request.head.repo.full_name") as repo_name,
      json_extract(payload, "$.number") as id,
      json_extract(payload, "$.action") as action,
      json_extract(payload, "$.pull_request.user.login") as author,
      json_extract(payload, "$.pull_request.commits") as commits,
      json_extract(payload, "$.pull_request.additions") as additions,
      json_extract(payload, "$.pull_request.deletions") as deletions,
      json_extract(payload, "$.pull_request.changed_files") as changed_files,
      json_extract(payload, "$.pull_request.review_comments") as review_comments,
      json_extract(payload, "$.pull_request.merged") as merged,
      json_extract(payload, "$.pull_request.created_at") as created_at,
      json_extract(payload, "$.pull_request.updated_at") as updated_at,
      json_extract(payload, "$.pull_request.closed_at") as closed_at,
      json_extract(payload, "$.pull_request.merged_at") as merged_at
    FROM '{input_table}'
    #WHERE instr(repo_name, 'airflow') > 0;
    """
    engine = sqlalchemy.engine.create_engine("sqlite:///github.db")
    rows = engine.execute(sql_statement).fetchall()
    return pd.DataFrame(rows)


def enrich(df):
    """Infer new information per row."""
    df = df[(df['merged'] == True) & (df["action"] == "closed")]
    df[["created_at", "closed_at"]] = df[["created_at", "closed_at"]].apply(pd.to_datetime)
    df['days_to_merge'] = (df['closed_at'] - df['created_at']).dt.days
    df['lines_diff'] = df['additions'] - df['deletions']
    return df


def load_from_pandas_dataframe(pandas_dataframe):
    """Load the data of interest to Sqlite database"""
    db = sqlalchemy.create_engine("sqlite:///github.db")
    pandas_dataframe.to_sql("enriched", db, if_exists="replace")


if __name__ == "__main__":
    table_name = load()
    transformed_rows = transform(table_name)
    enriched_df = enrich(transformed_rows)
    load_from_pandas_dataframe(enriched_df)