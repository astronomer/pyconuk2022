# Getting Started with Apache Airflow and the Astro SDK

This [workshop](https://pretalx.com/pycon-uk-2022/talk/PUA8SW/) was designed to introduce [Apache Airflow](https://airflow.apache.org/) to [PyCon UK 2022](https://2022.pyconuk.org/) attendees, on 17 September 2022. It was created by @kaxil and @tatiana on behalf of [Astronomer](https://astronomer.io).

In this tutorial, we'll use Apache Airflow together with the [Astro SDK](github.com/astronomer/astro-sdk/) to iterate through building Extract, Load and Transform pipelines.

The examples use two datasets (find out more about them [here](https://github.com/astronomer/astro-sdk/blob/main/python-sdk/tests/benchmark/datasets.md)):

* IMDB
* [Github archive](https://cloud.google.com/blog/topics/public-datasets/github-on-bigquery-analyze-all-the-open-source-code)


## Setup

### Option A: Astro CLI and Docker

The [Astro CLI](https://github.com/astronomer/astro-cli) is an open-source tool build by Astronomer which offers an abstraction on top of Airflow.
It aims to simplify the local development and cloud deployment of Airflow workflows.

By using this option, you will spin up a local environment having one container per Airflow component (webserver, scheduler and triggerer and database).

For more information, check the [docker](./docker) sub-directory.

**Dependencies**

* Internet connection
* [Git](https://git-scm.com/)
* [Docker](https://docs.docker.com/get-docker/), which comes with a pre-installed [Astro CLI](https://github.com/astronomer/astro-cli)

**How to setup**

1. Clone this repository

```bash
git clone https://github.com/astronomer/pyconuk2022.git
```

2. Start Airflow in containers by running a previously initialised project
```bash
cd pyconuk2022/docker
astro dev start
```

If successful this will output after a few minutes the following message:

```
Airflow is starting up! This might take a few minutes…

Project is running! All components are now available.

Airflow Webserver: http://localhost:8080
Postgres Database: localhost:5432/postgres
The default Airflow UI credentials are: admin:admin
The default Postgres DB credentials are: postgres:postgres
```

Prompting the web browser to open the Airflow webserver UI.


### Option B: Local Python virtual environment

**Dependencies:**

* Internet connection
* [Python](https://www.python.org/downloads/) 3.7 - 3.10
* [Pip](https://pypi.org/project/pip/) or your favourite [Python package management tool](https://packaging.python.org/en/latest/guides/tool-recommendations/)
* [SQLite](https://www.sqlite.org/)


**How to setup**

1. Clone this repository

```bash
  git clone https://github.com/astronomer/pyconuk2022.git
```

2. Create a Python virtual environment and activate it

```bash
  python3 -m venv /tmp/airflow-stable
  source /tmp/airflow-stable/bin/activate
```

3. Change directory to the local development

```bash
  cd local
```
  
4. Set environment variables

```bash
  source env.sh
```

5. Install the necessary python dependencies, including Airflow and the Astro SDK:

```bash
  pip install --upgrade pip
  pip install -r requirements_stable.txt
```

6. Initialise the Airflow database

```bash
  airflow db init
```

7. Create a SQLite database to run the example

```bash
# The sqlite_default connection has different host for MacOS vs. Linux
SQL_TABLE_NAME=`airflow connections get sqlite_default -o yaml | grep host | awk '{print $2}'` sqlite3 "$SQL_TABLE_NAME" "VACUUM;"
```

7. Spin up the Airflow webserver locally

```bash
  airflow standalone
```
