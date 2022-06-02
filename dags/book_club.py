from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from scripts.book_club_scripts import get_range_pages, get_book_info

default_args = {
    'start_date': datetime(2022, 5, 1),
    'retries': 5,
    'retry_delay': timedelta(seconds=120),
    'catchup': True,
}


@dag('book_club_two_pointo_dag', schedule_interval='30 12 * * *', max_active_runs=1, default_args=default_args,
     concurrency=5)
def book_club_dag():
    with TaskGroup('extract_and_load_pages') as extract_and_load:
        [
            PythonOperator(
                task_id=f"extract_page_{str(page).zfill(2)}",
                python_callable=get_book_info,
                op_kwargs={
                    "n_page": page,
                    "logical_date": "{{ ds_nodash }}"
                }
            ) for page in get_range_pages()
        ]

    end_tasks = EmptyOperator(
        task_id="End"
    )

    extract_and_load >> end_tasks


dag = book_club_dag()
