from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os
import pandas as pd



default_args = {
    "owner": "airflow",
    "retries": 0,
    "email_on_failure": False,
}

with DAG(
    dag_id='dag1',
    default_args=default_args,
    schedule_interval='0 16 * * *',
    start_date=datetime(2026, 4, 22),
    catchup=True,
) as dag:
    #task1
    wait_file = FileSensor(
        task_id='wait_file',
        filepath="/tmp/test/record_{{ (execution_date + macros.timedelta(hours=8)).strftime('%Y%m%d') }}.txt",
        poke_interval=60,
        timeout=30 * 60,
        mode="poke",
        soft_fail=False,
    )

    def check_file(**context):
            ti = context['ti']
            execu_date = context['execution_date']   
            real_date = execu_date + timedelta(hours=8)
            file_path = f"/tmp/test/record_{real_date.strftime('%Y%m%d')}.txt"

            errors = []

            try:
                file_size = os.path.getsize(file_path)
                if file_size == 0:
                     ti.xcom_push(key='error_message', value='\n'.join(errors))
                     raise ValueError('\n'.join(errors))
                
                if file_size <= 50:
                    errors.append(f'size < 50 bytes')
                    ti.xcom_push(key='error_message', value='\n'.join(errors))
                    raise ValueError('\n'.join(errors))
                
                df = pd.read_csv(file_path)
                if len(df) == 0:
                    errors.append(f'header 以外的資料筆數 =0')

                elif df['id'].duplicated().any():
                    errors.append(f'id 欄位值重複')

                if errors:
                    ti.xcom_push(key='error_message', value='\n'.join(errors))
                    raise ValueError("\n".join(errors))
                
            except Exception as e:
                    raise

    check_file_task = PythonOperator(
        task_id='check_file_task',
        python_callable=check_file,
        provide_context=True,
    )
    #task2
    send_email = EmailOperator(
         task_id = 'send_email',
         to = 'airflow@example.com',
         subject = 'Airflow - 排程失敗{{ ds }}',
         html_content = '''
            <h3>排程執行失敗通知</h3>
            <ul>
                <p><b>Task ID: </b>check_file_task</li>
                <p><b>排程執行日: </b>{{ ds }}</li>
                <p><b>Exception:</b>
                    {{ ti.xcom_pull(task_ids='check_file_task', key='error_message') }}
                </p>
            </ul>
        ''',
        trigger_rule = 'one_failed',
    )
    #task3
    move_file = BashOperator(
        task_id='move_file',
        bash_command="mv /tmp/test/record_{{ (execution_date + macros.timedelta(hours=8)).strftime('%Y%m%d') }}.txt /tmp/success/",
    )

wait_file >> check_file_task >> [send_email, move_file]