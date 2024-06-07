import yaml
from io import BytesIO
from minio import Minio
import time
import subprocess

def escape_quotes(value):
    if isinstance(value, str):
        return value.replace("'", "\\'")
    return value

def create_dag(topic_name, topic_data):
    owner = topic_data.get('owner', 'default_owner')
    scheduler_time = topic_data.get('scheduler_time', '0 0 * * *')
    tables_data = topic_data['tables']
    
    dag_content = f"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

def execute_script(script_path, **kwargs):
    command = f"python {{script_path}} --delta_format {{kwargs['delta_format']}} --key {{kwargs['key']}} --topic {{kwargs['topic']}} --kafka_server {{kwargs['kafka_server']}} --number_partitions {{kwargs['number_partitions']}} --number_of_instances {{kwargs['number_of_instances']}} --number_max_of_instances {{kwargs['number_max_of_instances']}} --driver_size {{kwargs['driver_size']}} --checkpoint_column {{kwargs['checkpoint_column']}} --executor_size {{kwargs['executor_size']}} --ignore_columns {{kwargs['ignore_columns']}} --schema '{{kwargs['schema']}}'"
    
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Script failed with error: {{result.stderr}}")
    else:
        print(f"Script executed successfully: {{result.stdout}}")

default_args = {{
    'owner': '{owner}',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}}

dag = DAG(
    'JEDI_{topic_name}',
    default_args=default_args,
    description='DAG para executar scripts para cada tabela dentro de {topic_name}',
    schedule_interval='{scheduler_time}',
    catchup=False,
)

"""
    for table_name, table_data in tables_data.items():
        args = {
            'delta_format': table_data.get('delta_format', 'None'),
            'key': table_data.get('key', 'None'),
            'topic': table_data.get('topic', 'None'),
            'kafka_server': table_data.get('kafka_server', 'None'),
            'number_partitions': table_data.get('number_partitions', 'None'),
            'number_of_instances': table_data.get('number_of_instances', 'None'),
            'number_max_of_instances': table_data.get('number_max_of_instances', 'None'),
            'driver_size': table_data.get('driver_size', 'None'),
            'checkpoint_column': table_data.get('checkpoint_column', 'None'),
            'executor_size': table_data.get('executor_size', 'None'),
            'ignore_columns': table_data.get('ignore_columns', 'None'),
            'schema': table_data.get('schema', 'None').replace('"', "'")
        }
        
        args = {k: escape_quotes(v) for k, v in args.items()}

        script_name = 'obi-wan.py' if args['topic'] != 'None' else 'anakin.py'
        dag_content += f"""
execute_{table_name}_task = PythonOperator(
    task_id='execute_{table_name}_task',
    provide_context=True,
    python_callable=execute_script,
    op_kwargs={{
        'script_path': '/opt/airflow/dags/jedi_files/{script_name}',
        'delta_format': '{args['delta_format']}',
        'key': "{args['key']}",
        'topic': '{args['topic']}',
        'kafka_server': '{args['kafka_server']}',
        'number_partitions': '{args['number_partitions']}',
        'number_of_instances': '{args['number_of_instances']}',
        'number_max_of_instances': '{args['number_max_of_instances']}',
        'driver_size': '{args['driver_size']}',
        'checkpoint_column': '{args['checkpoint_column']}',
        'executor_size': '{args['executor_size']}',
        'ignore_columns': "{args['ignore_columns']}",
        'schema': '{args['schema']}'
    }},
    dag=dag,
)
"""

        # Add send_trusted task for each table
        dag_content += f"""
send_trusted_{table_name}_task = PythonOperator(
    task_id='send_trusted_{table_name}_task',
    provide_context=True,
    python_callable=execute_script,
    op_kwargs={{
        'script_path': '/opt/airflow/dags/jedi_files/ahsoka.py',
        'delta_format': '{args['delta_format']}',
        'key': "{args['key']}",
        'topic': '{args['topic']}',
        'kafka_server': '{args['kafka_server']}',
        'number_partitions': '{args['number_partitions']}',
        'number_of_instances': '{args['number_of_instances']}',
        'number_max_of_instances': '{args['number_max_of_instances']}',
        'driver_size': '{args['driver_size']}',
        'checkpoint_column': '{args['checkpoint_column']}',
        'executor_size': '{args['executor_size']}',
        'ignore_columns': "{args['ignore_columns']}",
        'schema': '{args['schema']}'
    }},
    dag=dag,
)

execute_{table_name}_task >> send_trusted_{table_name}_task
"""

    return f"JEDI_{topic_name}.py", dag_content

def upload_to_minio():
    minio_client = Minio(
        endpoint="172.26.0.3:9000",
        access_key="DqKP9jCEWxoHZOwMeaha",
        secret_key="pu8AfOYua8KDwfdqUDGuwFfzUb1hPIKbD6aMeWQL",
        secure=False
    )
    with open("/app/topics/minio/topics_minio.yaml", 'r') as stream:
        try:
            topics_data = yaml.safe_load(stream)
            for topic, data in topics_data.items():
                topic_name = topic
                file_name, dag_content = create_dag(topic_name, data)
                file_data = BytesIO(dag_content.encode())
                bucket_name = 'prd-dags-jedi'
                minio_client.put_object(bucket_name, f"{topic_name}/{file_name}", file_data, file_data.getbuffer().nbytes)
        except yaml.YAMLError as exc:
            print(exc)

def main():
    while True:
        upload_to_minio()
        time.sleep(15)

if __name__ == "__main__":
    main()
