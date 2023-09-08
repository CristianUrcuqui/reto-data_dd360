from contextlib import closing
from datetime import datetime, timedelta
from copy import deepcopy
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import ProgrammingError, DatabaseError


POSTGRES_CONN_ID = 'curso_postgres_conn'

def get_default_args() -> dict:
    default_args = {
        'owner': '',
        'email': [''],
        'email_on_failure': False,
        'email_on_retry': False,
        'on_failure_callback': on_error_call,
        'on_success_callback': on_success_call,
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }
    return deepcopy(default_args)

def execute_postgres(sql, postgres_conn_id, with_cursor=False):
    hook_connection = PostgresHook(postgres_conn_id=postgres_conn_id)
    with closing(hook_connection.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            try:
                cur.execute(sql)
                conn.commit()  # Committing the transaction
            except DatabaseError as e:
                print(f"Database Error: {str(e)}")
                conn.rollback()  # Rollback in case there is any error
                return None
            
            try:
                res = cur.fetchall()
            except ProgrammingError:
                print("No results to fetch from the query.")
                res = None
            
            if with_cursor:
                return (res, cur)
            else:
                return res

def update_logs(details):
    sql = """
        SELECT COUNT(1)
        FROM AIRFLOW_TASK_LOGS
        WHERE dag = '{dag}' AND task = '{task}'
        """.format(
        dag=details['dag'],
        task=details['task']
    )
    count = execute_postgres(sql, POSTGRES_CONN_ID)[0][0]
    sql = ""
    if count != 0:
        # Update the existing log
        sql = """
            UPDATE AIRFLOW_TASK_LOGS
            SET
                    dag = '{dag}',
                    task = '{task}',
                    last_timestamp = '{timestamp}',
                    state = '{state}',
                    exception = '{exception}',
                    owner = '{owner}',
                    finished_at = '{finished_at}',
                    duration = '{duration}',
                    operator = '{operator}',
                    cron = '{cron}'
            WHERE dag = '{dag}' AND task = '{task}'
        """.format(
            dag=details['dag'],
            task=details['task'],
            timestamp=details['timestamp'],
            exception=details['exception'],
            state=details['state'],
            owner=details['owner'],
            finished_at=details['finished_at'],
            duration=details['duration'],
            operator=details['operator'],
            cron=details['cron']
        )
    else:
        # Insert a new log
        sql = """
            INSERT INTO AIRFLOW_TASK_LOGS
        (
            DAG,
            TASK,
            LAST_TIMESTAMP,
            STATE,
            EXCEPTION,
            OWNER,
            FINISHED_AT,
            DURATION,
            OPERATOR,
            CRON
        )
        VALUES(
            '{dag}',
            '{task}',
            '{timestamp}',
            '{state}',
            '{exception}',
            '{owner}',
            '{finished_at}',
            '{duration}',
            '{operator}',
            '{cron}'
        )
        """.format(
            dag=details['dag'],
            task=details['task'],
            timestamp=details['timestamp'],
            exception=details['exception'],
            state=details['state'],
            owner=details['owner'],
            finished_at=details['finished_at'],
            duration=details['duration'],
            operator=details['operator'],
            cron=details['cron']
        )
    print(sql)
    execute_postgres(sql, POSTGRES_CONN_ID)

def set_alert_handling(context):
    print(context)
    fixed_start_utc = datetime.fromtimestamp(context['task_instance'].start_date.timestamp()).replace(tzinfo=None)
    fixed_start = str(fixed_start_utc - timedelta(hours=5))
    if context['task_instance'].duration is not None:
        fixed_end_utc = datetime.fromtimestamp(context['task_instance'].end_date.timestamp()).replace(tzinfo=None)
        fixed_end = str(fixed_end_utc - timedelta(hours=5))
        duration = context['task_instance'].duration
    else:
        fixed_end_utc = datetime.utcnow()
        fixed_end = str(fixed_end_utc - timedelta(hours=5))
        duration = str((fixed_end_utc - fixed_start_utc).total_seconds())
    details = {
        'dag': context['dag'].dag_id,
        'task': context['task'].task_id,
        'timestamp': fixed_start,
        'state': context['state'],
        'owner': context['dag'].default_args['owner'],
        'finished_at': fixed_end,
        'duration': duration,
        'operator': context['task_instance'].operator,
        'cron': context['dag'].schedule_interval
    }
    if 'exception' in context:
        details['exception'] = context['exception']
        if 'exception_details' in context:
            details['exception'] += '\n Details: ' + context['exception_details']
    else:
        details['exception'] = 'N/A'
    if details['state'] == 'failed':
        print('Oops, something went wrong:', details)
    
    return update_logs(details)

def on_error_call(context):
    context['state'] = 'failed'
    set_alert_handling(context)

def on_success_call(context):
    context['state'] = 'success'
    set_alert_handling(context)
