from contextlib import closing
from datetime import datetime, timedelta
import io
from copy import deepcopy
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


SNOWFLAKE_CONN_ID = 'snowflake_conn_id'


def get_default_args() -> dict:
    """Initialize default arguments."""
    default_args = {
        'owner': '',
        'email': [''],
        'email_on_failure': False,  # Alerting handled with python
        'email_on_retry': False,  # Alerting handled with python
        'on_failure_callback': on_error_call,
        'on_success_callback': on_success_call,
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }
    return deepcopy(default_args)


def execute_snowflake(sql, snowflake_conn_id, with_cursor=False):
    """Execute snowflake query."""
    hook_connection = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id)
    with closing(hook_connection.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(sql)
            res = cur.fetchall()
            if with_cursor:
                return (res, cur)
            else:
                return res


def update_logs(details):
    """Update log queries."""
    sql = """
        select count(1)
        from CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.AIRFLOW_TASK_LOGS 
        where dag = '{dag}' and task = '{task}'
        """.format(
        dag=details['dag'],
        task=details['task']
    )
    count = execute_snowflake(sql, SNOWFLAKE_CONN_ID)[0][0]
    sql = ""
    if count != 0:
        sql = """
            UPDATE CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.AIRFLOW_TASK_LOGS 
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
            WHERE dag = '{dag}' and task = '{task}'
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
        sql = """
            insert into CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.AIRFLOW_TASK_LOGS 
        (
            DAG
            ,TASK
            ,LAST_TIMESTAMP
            ,STATE
            ,EXCEPTION
            ,OWNER
            ,FINISHED_AT
            ,DURATION
            ,OPERATOR
            ,CRON
        )
        values(
            '{dag}'
            , '{task}'
            , '{timestamp}'
            , '{state}'
            , '{exception}'
            , '{owner}'
            , '{finished_at}'
            , '{duration}'
            , '{operator}'
            , '{cron}'
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
    execute_snowflake(sql, SNOWFLAKE_CONN_ID)



def set_alert_handling(context):
    """Set alerts handling."""
    print(context)
    fixed_start_utc = datetime.fromtimestamp(context['task_instance'].start_date.timestamp()).replace(tzinfo=None)
    fixed_start = str(fixed_start_utc-timedelta(hours=5))
    if context['task_instance'].duration is not None:
        fixed_end_utc = datetime.fromtimestamp(context['task_instance'].end_date.timestamp()).replace(tzinfo=None)
        fixed_end = str(fixed_end_utc-timedelta(hours=5))
        duration = context['task_instance'].duration
    else:
        fixed_end_utc = datetime.utcnow()
        fixed_end = str(fixed_end_utc-timedelta(hours=5))
        duration = str((fixed_end_utc-fixed_start_utc).total_seconds())
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
    """Set falied state and call alert handling."""
    context['state'] = 'failed'
    set_alert_handling(context)


def on_success_call(context):
    """Set falied state and call alert handling."""
    context['state'] = 'success'
    set_alert_handling(context)