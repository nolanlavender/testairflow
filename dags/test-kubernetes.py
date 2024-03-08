from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pendulum
from airflow.models import Variable
import yaml
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule


tenantId = Variable.get("AZURE_TENANT_ID") 
clientSecret = Variable.get("AZURE_CLIENT_SECRET")
clientId= Variable.get("AZURE_CLIENT_ID")

default_args = {
    'owner': 'Nolan',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Define your custom function to modify the YAML file
def modify_yaml_file():
    file_path = '/usr/local/airflow/dags/job_yaml/chi-gpt-crawl.yaml'

    # Get the current datetime
    current_datetime = datetime.now()

    # Format the datetime as a string
    datetime_string = current_datetime.strftime("%Y-%m-%d-%H-%M-%S")


    with open(file_path, 'r') as file:
        original_yaml = yaml.safe_load(file)

    # Modify the YAML content as needed
    original_yaml['metadata']['name'] = 'chi-server-crawl'+datetime_string

    with open(file_path, 'w') as file:
        yaml.dump(original_yaml, file)

with DAG(
    'test-kube',
    default_args=default_args,
    start_date=datetime(2024, 2, 24),
    schedule_interval=None,#timedelta(days=1),
    catchup=False
) as dag:
    # Define a task using the PythonOperator
    modify_yaml_task = PythonOperator(
        task_id='modify_yaml_task',
        python_callable=modify_yaml_file,
        dag=dag,
    )
   
    
    submit_job = BashOperator(
    task_id='chi-gpt-crawl',
    bash_command="""az login --service-principal -u {} -p {} --tenant {} &&
                    az account set --subscription 'Pay-As-You-Go(Converted to EA)' &&
                    az aks get-credentials --resource-group DataScience-Jobs --name data-science-jobe --admin &&
                    kubectl apply -f /usr/local/airflow/dags/job_yaml/chi-gpt-crawl.yaml
                  """.format(clientId, clientSecret, tenantId)
    )

    # restart_dag = TriggerDagRunOperator(
    #     task_id="restart_dag",
    #     trigger_dag_id='test-kube',
    #     trigger_rule=TriggerRule.ONE_FAILED,
    #     dag=dag,
    # )
    
    modify_yaml_task >> submit_job

    # modify_yaml_task >> submit_job >> restart_dag
    # modify_yaml_task >> restart_dag