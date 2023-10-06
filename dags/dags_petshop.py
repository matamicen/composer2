import datetime
import json
import logging

import airflow
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import \
    GoogleCloudStorageToBigQueryOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.http_operator import \
    SimpleHttpOperator  # Import the SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

## set global variables fro the JSON file
json_var = Variable.get('customer_name_1')
PARSED_JSON = json.loads(json_var)
COMPANY_NAME = PARSED_JSON.get("company_name")
PROJECT_NAME = PARSED_JSON.get("bq_project_name")



# Define a success callback function
def send_success_email(context):
    subject = 'Task Succeeded'
    to = ['matamicen@gmail.com']
    html_content = f"The task {context['task_instance']} has succeeded."
    send_email(to=to, subject=subject, html_content=html_content)

    # Function to read JSON from the Airflow Variable and log it
def read_json_and_log():
        try:
            # Get the JSON variable
            # json_variable = Variable.get('customer_name_1', deserialize_json=True)
            json_variable = Variable.get('customer_name_1')

            # Parse the JSON content
            parsed_json = json.loads(json_variable)

            # Log the JSON content
            logging.info("Company Name: %s", parsed_json.get("company_name"))
            logging.info("BQ Project Name: %s", parsed_json.get("bq_project_name"))
            logging.info("BQ Dataset: %s", parsed_json.get("bq_dataset"))
            logging.info("Files:")
            for file_info in parsed_json.get("files", []):
                logging.info("  Name: %s", file_info.get("name"))
                logging.info("  Example: %s", file_info.get("example"))
                logging.info("  GCS Bucket: %s", file_info.get("gcs_bucket"))
                logging.info("  Temporary Table: %s", file_info.get("tmp_table"))
                logging.info("  Consolidation Table: %s", file_info.get("consolidation_table"))
                logging.info("-" * 40)

            return parsed_json  # Return the parsed_json variable


        except Exception as e:
            logging.error("Error reading and logging JSON: %s", str(e))

# Function to call the Google Cloud Function with parsed_json as a parameter
def call_google_cloud_function(**kwargs):
    # esto funcionaba perfecto para leer el Xcomm de la task anterior, pero para optimizar vamos a leer el JSON desde esta task directamente
    # parsed_json = kwargs['ti'].xcom_pull(task_ids='read_json_and_log')

    try:

        # Get the JSON variable
        # json_variable = Variable.get('customer_name_1', deserialize_json=True)
        # json_variable = Variable.get('customer_name_1')
        # Parse the JSON content
        # parsed_json = json.loads(json_variable)
        # Define the Google Cloud Function parameters
        function_url = 'ne-gcp-stance_tmp_schema_validations-dev'  # Replace with your function URL
        headers = {"Content-Type": "application/json"}  # Specify content type
        logging.info("json parsed es: %s", PARSED_JSON)
        logging.info("bq_project_name antes de enviar: %s", PARSED_JSON.get("bq_project_name"))

        # Convert the parsed_json to a JSON string
        # json_data = json.dumps({"parsed_json": parsed_json})
        # json_data = json.dumps( PARSED_JSON)
        # logging.info("json_data antes de enviar: %s", json_data)

        fullname = kwargs['dag_run'].conf['bucket'] + "/" + kwargs['dag_run'].conf['name']
        file_path = kwargs['dag_run'].conf['name']

        logging.info("fullname: %s", fullname)

        data = {
            "fullfilename": fullname,
            "file_path": file_path,
            "dagid": kwargs['dag'].dag_id,
            "runid": kwargs['dag_run'].run_id,
            "json_config": PARSED_JSON
        }
#

        json_data = json.dumps(data)

        # Call the Google Cloud Function using SimpleHttpOperator
        http_operator = SimpleHttpOperator(
            task_id='calling_tmp_schema_process',
          #  http_conn_id='http_default',  # Specify the HTTP connection
            http_conn_id='http_cloud_functions',
            endpoint=function_url,
            method='POST',  # Use POST method to send JSON data
            headers=headers,
            data=json_data,
            dag=dag,
        )
   #     http_operator.execute(context={})
                # Execute the HTTP operator
        http_response = http_operator.execute(context={})

        logging.info("HTTP response content: %s", http_response)

        return http_response


    except Exception as e:
        # logging.error("Error calling Google Cloud Function: %s", str(e))
        raise AirflowException("An error occurred: {0}".format(str(e)))


# Function to call the Google Cloud Function with parsed_json as a parameter
def call_google_cloud_function2(**kwargs):
    # esto funcionaba perfecto para leer el Xcomm de la task anterior, pero para optimizar vamos a leer el JSON desde esta task directamente
    # parsed_json = kwargs['ti'].xcom_pull(task_ids='read_json_and_log')

    try:

        # Define the Google Cloud Function parameters
        function_url = 'ne-gcp-stance-consolidation_schema_validations-dev'  # Replace with your function URL
        headers = {"Content-Type": "application/json"}  # Specify content type
        logging.info("json parsed es: %s", PARSED_JSON)
        logging.info("bq_project_name antes de enviar: %s", PARSED_JSON.get("bq_project_name"))

        fullname = kwargs['dag_run'].conf['bucket'] + "/" + kwargs['dag_run'].conf['name']

        logging.info("fullname: %s", fullname)

        data = {
            "fullfilename": fullname,
            "dagid": kwargs['dag'].dag_id,
            "runid": kwargs['dag_run'].run_id,
            "json_config": PARSED_JSON
        }
#

        json_data = json.dumps(data)
        logging.info("json_data antes de enviar: %s", json_data)


        # Call the Google Cloud Function using SimpleHttpOperator
        http_operator = SimpleHttpOperator(
            task_id='calling_tmp_schema_process',
          #  http_conn_id='http_default',  # Specify the HTTP connection
            http_conn_id='http_cloud_functions',
            endpoint=function_url,
            method='POST',  # Use POST method to send JSON data
            headers=headers,
            data=json_data,
            dag=dag,
        )

        http_response = http_operator.execute(context={})

        logging.info("HTTP response content: %s", http_response)

        return http_response


    except Exception as e:
        # logging.error("Error calling Google Cloud Function: %s", str(e))
        raise AirflowException("An error occurred: {0}".format(str(e)))


def set_schema_and_load(**kwargs):
    ti = kwargs['ti']
    schema_aux = ti.xcom_pull(task_ids='call_google_cloud_function')
    schema = json.loads(schema_aux)

    dag_run = kwargs['dag_run']
    filename = dag_run.conf.get('name')
    bucket = dag_run.conf.get('bucket')

    logging.info("filename: %s", filename)
    logging.info("bucket: %s", bucket)
    logging.info("Mat COMPANY_NAME: %s", COMPANY_NAME)
    logging.info("Mat project_name: %s",PROJECT_NAME)


    # GCS to BigQuery data load Operator and task
    gcs_to_bq_load_2 = GoogleCloudStorageToBigQueryOperator(
                task_id='gcs_to_bq_load_2',
                # bucket='newengen_trigger_test',
                # source_objects='folder1/Copy 7of second_reverse_1.csv',
                bucket=bucket,
                source_objects=[filename],
                destination_project_dataset_table='ne-discoclients.petshop.campaigns',
                schema_fields= schema,
                # schema_fields=schema2,  # Pull the schema from XCom
                skip_leading_rows=1,
                on_success_callback=send_success_email,  # Attach the success callback function
                create_disposition='CREATE_IF_NEEDED',
                #create_disposition='CREATE_NEVER',
                write_disposition='WRITE_TRUNCATE',
    dag=dag)

    # Execute the inner operator
    gcs_to_bq_load_2.execute(context=kwargs)

def insert_to_consolidation(**kwargs):
    ti = kwargs['ti']
    sql_query = ti.xcom_pull(task_ids='call_google_cloud_function2')
    # schema = json.loads(schema_aux)

    dag_run = kwargs['dag_run']
    filename = dag_run.conf.get('name')
    bucket = dag_run.conf.get('bucket')

    # sql_query_insert = """
    #     INSERT INTO `ne-discoclients.petshop.consolidation`
    #     (
    #     id,
    #     day,
    #     campaign_name,
    #     description,
    #     clicks,
    #     impressions,
    #     spend,
    #     country,
    #     colextra2,
    #     colextra1,
    #     filename,
    #     dag_id,
    #     run_id
    #     )
    #     SELECT
    #     id,
    #     day,
    #     campaign_name,
    #     description,
    #     clicks,
    #     impressions,
    #     spend,
    #     country,
    #     colextra2,
    #     FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS colextra1,
    #     '{{ dag_run.conf['bucket'] }}/{{ dag_run.conf['name'] }}' AS filename,
    #     '{{ dag.dag_id }}' AS dag_id,
    #     '{{ dag_run.run_id }}' AS run_id
    #     FROM `ne-discoclients.petshop.campaigns`



    #  """



    # BigQuery task, operator
    create_aggr_bq_table = BigQueryOperator(
    task_id='create_aggr_bq_table',
    use_legacy_sql=False,
    allow_large_results=True,
    sql=sql_query,
    # sql= sql_query_insert,
    dag=dag)

    # # Execute the inner operator
    create_aggr_bq_table.execute(context=kwargs)

    # logging.info("filename: %s", filename)
    # logging.info("bucket: %s", bucket)
    # logging.info("Mat COMPANY_NAME: %s", COMPANY_NAME)
    # logging.info("Mat project_name: %s",project_name)


    # GCS to BigQuery data load Operator and task
    # gcs_to_bq_load_2 = GoogleCloudStorageToBigQueryOperator(
    #             task_id='gcs_to_bq_load_2',
    #             # bucket='newengen_trigger_test',
    #             # source_objects='folder1/Copy 7of second_reverse_1.csv',
    #             bucket=bucket,
    #             source_objects=[filename],
    #             destination_project_dataset_table='ne-discoclients.petshop.campaigns',
    #             schema_fields= schema,
    #             # schema_fields=schema2,  # Pull the schema from XCom
    #             skip_leading_rows=1,
    #             on_success_callback=send_success_email,  # Attach the success callback function
    #             create_disposition='CREATE_IF_NEEDED',
    #             #create_disposition='CREATE_NEVER',
    #             write_disposition='WRITE_TRUNCATE',
    # dag=dag)

    # # Execute the inner operator
    # gcs_to_bq_load_2.execute(context=kwargs)




with airflow.DAG(
    "petshop_dag",
    start_date=datetime.datetime(2021, 1, 1),
    max_active_runs=1,
    # Not scheduled, trigger only
    schedule_interval=None,
) as dag:
    # Print the dag_run's configuration, which includes information about the
    # Cloud Storage object change.
    # print_gcs_info = BashOperator(
    #     # task_id="print_gcs_info", bash_command="echo {{ dag_run.conf }}"
    #     task_id="print_gcs_info", bash_command="echo {{ dag_run.conf['name'] }}"
    # )


        # Create a PythonOperator to execute the read_json_and_log function
    # read_json_task = PythonOperator(
    #     task_id='read_json_and_log',
    #     python_callable=read_json_and_log,
    #     provide_context=True,  # Add this line to provide context to the function
    #     dag=dag,
    # )

    # Create a PythonOperator to call the Google Cloud Function
    call_gcf_task = PythonOperator(
        task_id='call_google_cloud_function',
        python_callable=call_google_cloud_function,
        provide_context=True,  # Add this line to provide context to the function
        dag=dag,
    )


    # filename = "{{ dag_run.conf['name'] }}"
    # bucket = "{{ dag_run.conf['bucket'] }}"

    # reverse description & campaign
    # schema = [
    #                             {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'day', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'campaign_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'clicks', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'impressions', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'spend', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'colextra2', 'type': 'STRING', 'mode': 'NULLABLE'}
    #                           ]

    # schema = [
    #                             {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'day', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'campaign_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'clicks', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'impressions', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'spend', 'type': 'STRING', 'mode': 'NULLABLE'},
    #                             {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'}
    #                           ]



    gcs_to_bq_load = PythonOperator(
            task_id='gcs_to_bq_load',
            python_callable=set_schema_and_load,
            provide_context=True,
            dag=dag,
        )

    # # GCS to BigQuery data load Operator and task
    # gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
    #             task_id='gcs_to_bq_load',
    #             bucket=bucket,
    #             source_objects=[filename],
    #             destination_project_dataset_table='learned-pier-398000.petshop.campaigns',
    #             # schema_fields= schema,
    #             schema_fields={{ti.xcom_pull(task_ids='call_google_cloud_function') }},  # Pull the schema from XCom
    #             skip_leading_rows=1,
    #             on_success_callback=send_success_email,  # Attach the success callback function
    #             # create_disposition='CREATE_IF_NEEDED',
    #             create_disposition='CREATE_NEVER',
    #             write_disposition='WRITE_TRUNCATE',
    # dag=dag)

    sql_query_delete_consolidation = """
    DELETE
FROM ne-discoclients.petshop.consolidation AS c
WHERE DAY BETWEEN (
  SELECT MIN(DAY)
  FROM ne-discoclients.petshop.campaigns
) AND (
  SELECT MAX(DAY)
  FROM ne-discoclients.petshop.campaigns
)
"""

    # BigQuery task, operator
    delete_consolidation_bq_table = BigQueryOperator(
    task_id='delete_consolidation',
    use_legacy_sql=False,
    allow_large_results=True,
    sql= sql_query_delete_consolidation,
    dag=dag)





    # Create a PythonOperator to call the Google Cloud Function to modify consolidation schema if necccesary
    call_gcf_consolidation = PythonOperator(
        task_id='call_google_cloud_function2',
        python_callable=call_google_cloud_function2,
        provide_context=True,  # Add this line to provide context to the function
        dag=dag,
    )


    insert_to_consolidation = PythonOperator(
            task_id='insert_to_consolidation',
            python_callable=insert_to_consolidation,
            provide_context=True,
            dag=dag,
        )


#     sql_query_insert = """
# INSERT INTO `ne-discoclients.petshop.consolidation`
# (
#   id,
#   day,
#   campaign_name,
#   description,
#   clicks,
#   impressions,
#   spend,
#   country,
#   colextra2,
#   colextra1,
#   filename,
#   dag_id,
#   run_id
# )
# SELECT
#   id,
#   day,
#   campaign_name,
#   description,
#   clicks,
#   impressions,
#   spend,
#   country,
#   colextra2,
#   FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS colextra1,
#   '{{ dag_run.conf['bucket'] }}/{{ dag_run.conf['name'] }}' AS filename,
#   '{{ dag.dag_id }}' AS dag_id,
#   '{{ dag_run.run_id }}' AS run_id
# FROM `ne-discoclients.petshop.campaigns`



#     """



#     # BigQuery task, operator
#     create_aggr_bq_table = BigQueryOperator(
#     task_id='create_aggr_bq_table',
#     use_legacy_sql=False,
#     allow_large_results=True,
#     sql= sql_query_insert,
#     dag=dag)


# funcionando este operator que envia mail
    task_email = EmailOperator(
        task_id="send-email",
        # conn_id="sendgrid_default",
        # You can specify more than one recipient with a list.
        to="matamicen@gmail.com",
        subject="EmailOperator test for SendGrid",
        html_content="This is a test message sent through SendGrid local.",
        dag=dag,
    )

# Settting up task  dependency
# esta linea de ejecucion estaba funcionando, ahora vamos a sacar lo innecsario
# task_email >> print_gcs_info >> read_json_task >> call_gcf_task >> gcs_to_bq_load >> delete_consolidation_bq_table >> create_aggr_bq_table


task_email >> call_gcf_task >> gcs_to_bq_load >> call_gcf_consolidation >> delete_consolidation_bq_table >> insert_to_consolidation