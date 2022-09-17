FROM docker.io/kaxil/astro-runtime-dev:6.0.0-alpha11-with-sdk

ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
ENV AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE=True
ENV OPENLINEAGE_DISABLED=True

# Create Sqlite connection
ENV AIRFLOW_CONN_SQLITE_DEFAULT=sqlite:///tmp/sqlite_default.db
# Create Postgres connection
ENV AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://postgres:postgres@postgres:5432/postgres
