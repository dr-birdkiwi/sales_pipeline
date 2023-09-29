
import pathlib
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sales_pipeline.tasks.extract_load import (add_store_country_city,
                                               insert_only, remove_file, retrieve_users,
                                               retrieve_weather, truncate_load)


@dag(
    schedule='@daily',
    start_date=pendulum.datetime(2023, 9, 26, tz="UTC"),
    catchup=False,
    tags=["sales", "pipeline"],
)
def sales_pipeline():
    """
    ### Sales Pipeline Documentation
    This is an ELT pipeline.
    EL: sales, users, weather are extracted and loaded into staging tables
    T: staging tables are processed and transformed into various data models via SQL
    """

    @task
    def el_sales():
        source_file = pathlib.Path(__file__).parent.resolve() / 'data/sales_data.csv'
        sink_file = '/tmp/sales_data_store.csv'
        add_store_country_city(source_file, sink_file)
        truncate_load('stg_sales', sink_file)
        remove_file(sink_file)

    @task
    def el_users():
        sink_file = '/tmp/users.csv'
        retrieve_users(sink_file)
        truncate_load('stg_users', sink_file)
        remove_file(sink_file)

    @task
    def el_weather():
        sink_file = '/tmp/weather.csv'
        retrieve_weather(sink_file)
        insert_only('stg_weather', sink_file)
        remove_file(sink_file)

    db_init = PostgresOperator(
        task_id="db_init",
        postgres_conn_id="postgres_default",
        sql="sql/ddl.sql",
    )

    db_transform = PostgresOperator(
        task_id="db_transform",
        postgres_conn_id="postgres_default",
        sql="sql/transformation.sql",
    )

    db_init >> el_sales() >> el_users() >> el_weather() >> db_transform


sales_pipeline()
