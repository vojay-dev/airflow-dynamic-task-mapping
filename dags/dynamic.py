from datetime import datetime, timedelta
from typing import List, Any

from airflow.decorators import dag, task
from airflow.models.param import Param


@dag(
    schedule=None,  # no start_date needed when schedule is set to None
    dag_display_name='🚀 Dynamic Task Mapping Demo 💚',
    params={
        'mode': Param(
            default='daily',
            enum=['daily', 'custom'],
            description='Execution mode'
        ),
        'start_date': Param(
            default='2024-01-01',
            format='date',
            description='Start date for the date range (only for custom)'
        ),
        'end_date': Param(
            default='2024-01-03',
            format='date',
            description='End date for the date range (only for custom)'
        ),
        'events_per_partition': Param(
            default=10,
            type='integer',
            description='Random events per partition'
        )
    }
)
def dynamic_task_mapping_demo():
    @task(task_display_name='📆 Generate Dates')
    def get_date_range(
            start_date: str,
            end_date: str,
            params: dict[str, Any],
            ds: str
    ) -> List[str]:
        """
        You can access Airflow context variables by adding them as keyword arguments,
        like params and ds
        """
        if params['mode'] == 'daily':
            return [ds]

        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        return [
            (start + timedelta(days=i)).strftime('%Y-%m-%d')
            for i in range((end - start).days + 1)
        ]

    @task.virtualenv(
        task_display_name='🔥 Process Partitions',
        requirements=['duckdb>=0.9.0'],
        python_version='3.12',
        map_index_template='{{ "Partition: " ~ task.op_kwargs["date_str"] }}'
    )
    def process_partition(date_str: str, events_per_partition: int):
        import duckdb

        # write sample data for this partition
        with duckdb.connect(':memory:') as conn:
            conn.execute(f"""
                COPY (
                    SELECT
                        strftime(DATE '{date_str}', '%Y-%m-%d') AS event_date,
                        (random() * 1000)::INTEGER AS some_value
                    FROM range({events_per_partition})
                ) TO 'out' (
                    FORMAT PARQUET,
                    PARTITION_BY (event_date),
                    OVERWRITE_OR_IGNORE
                )
            """)

    dates = get_date_range(
        start_date='{{ params.start_date }}',
        end_date='{{ params.end_date }}'
    )

    # partial: same for all task instances
    # expand: different for each instance (list of values, one for each instance)
    process_partition.partial(
        events_per_partition='{{ params.events_per_partition }}'
    ).expand(
        date_str=dates
    )

dynamic_task_mapping_demo()
