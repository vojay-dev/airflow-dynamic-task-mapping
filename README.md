# Dynamic Task Mapping Demo

This project showcases how to use Dynamic Task Mapping in Apache Airflow.

For details, please read my article: [Stop Creating Multiple Airflow DAGs for Reloads and Parallel Processing](https://vojay.de/2024/11/22/airflow-dynamic/)

In this article, we’ll tackle a common challenge in Airflow development: the proliferation of nearly identical DAGs for
handling different data processing scenarios, especially those involving partitioned tables and historical reloads.
You’ll learn how to build a single, flexible DAG that leverages Dynamic Task Mapping to process partitions in parallel,
handling both daily operations and custom date range reloads with ease.

## Setup

1. Start Airflow on your local machine by running `astro dev start`.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running `docker ps`.

Note: Running `astro dev start` will start your project with the Airflow Webserver exposed at port 8080 and Postgres
exposed at port 5432.

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with `admin` as user and password.
