from datetime import datetime

from airflow import DAG
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

input_file = File(
    path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
)

imdb_movies_table = Table(name="imdb_movies", conn_id="postgres_default")

top_animations_table = Table(name="top_animation", conn_id="postgres_default")
START_DATE = datetime(2022, 9, 1)


@aql.transform()
def get_top_five_animations(input_table: Table):
    return """
        SELECT title, rating
        FROM {{input_table}}
        WHERE genre1='Animation'
        ORDER BY rating desc
        LIMIT 5;
    """


with DAG(
    dag_id="example_dag",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
) as load_dag:
    imdb_movies = aql.load_file(
        input_file=input_file,
        task_id="load_csv",
        output_table=imdb_movies_table,
    )

    top_five_animations = get_top_five_animations(
        input_table=imdb_movies,
        output_table=top_animations_table,
    )

    aql.export_file(
        task_id="save_dataframe_to_gcs",
        input_data=top_five_animations,
        output_file=File(path="/tmp/top_5_movies.csv"),
        if_exists="replace",
    )
