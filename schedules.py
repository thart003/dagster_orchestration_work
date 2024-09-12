from .assets import mysql_to_clickhouse_job
from dagster import schedule, DefaultScheduleStatus, RunRequest

@schedule(
    job=mysql_to_clickhouse_job,
    cron_schedule="0 8 * * *",
    name="mysql_to_clickhouse_schedule",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED
)

def mysql_to_clickhouse_schedule():
    return RunRequest(
        job_name="mysql_to_clickhouse_job",
    )
