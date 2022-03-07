import os
import shutil
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context
from airflow import settings
from airflow.models import Connection


def create_spark_connection_if_not_exists(conn_id="spark_docker", host="spark://spark-master", port=7077):
    conn = Connection(conn_id=conn_id,
                      conn_type="spark",
                      host=host,
                      port=port)
    session = settings.Session
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if conn_name is None:
        session.add(conn)
        session.commit()


def not_empty(field):
    return {"field": field,
            "op": F"col(\"{field}\").cast(StringType())!=lit(\"\")",
            "invert_op": F"col(\"{field}\").cast(StringType())==lit(\"\")",
            "fail_reason": F"\"{field} is empty\""}


def not_null(field):
    return {"field": field,
            "op": F"col(\"{field}\").isNotNull()",
            "invert_op": F"col(\"{field}\").isNull()",
            "fail_reason": F"\"{field} is null\""}


def current_timestamp(name):
    return F"withColumn(\"{name}\",current_timestamp())"


switch_validation = {
    "notEmpty": not_empty,
    "notNull": not_null
}

switch_addfields = {
    "current_timestamp": current_timestamp
}

def generate_filter_operators():
    return


def process_transformation_validation(transformations, lines, input_df):
    operations = []
    name_out_df = transformations["name"]
    for validation in transformations["params"]["validations"]:
        field = validation["field"]
        for validation_types in validation["validations"]:
            operations += [switch_validation.get(validation_types, "")(field)]
    filter_content = [operation["op"] for operation in operations]
    filter_content = "(" + ") & (".join(filter_content) + ")" if len(filter_content) > 1 else filter_content[0]

    filter_msg = [F"when({operation['invert_op']},{operation['fail_reason']})" for operation in operations]
    filter_msg = ','.join(filter_msg)

    lines += [F"{name_out_df}_ok={input_df}.filter({filter_content})"]
    lines += [F"{name_out_df}_ko={input_df}.filter(~({filter_content}))"]
    lines += [F"{name_out_df}_ko={name_out_df}_ko.withColumn('arraycoderrorbyfield',array({filter_msg}))"]
    lines += [F"{name_out_df}_ko={name_out_df}_ko.withColumn('arraycoderrorbyfield',"
              + "array_except('arraycoderrorbyfield',array(lit(None))))"]
    return lines


def process_transformation_add_fields(transformations, lines, input_df):
    operations = []
    name_out_df = transformations["name"]
    for addfields in transformations["params"]["addFields"]:
        name = addfields["name"]
        function_type = addfields["function"]
        operations += [switch_addfields.get(function_type, "")(name)]
    add_fields = ".".join(operations)
    lines += [F"{name_out_df}={input_df}.{add_fields}"]
    return lines


@task()
def generate_transformations_code(data):
    dataflow = data["dataflow"]
    lines = data["lines"]
    for transformations in dataflow["transformations"]:
        input_df = transformations["params"]["input"]
        if transformations["type"] == "validate_fields":
            lines = process_transformation_validation(transformations, lines, input_df)
        if transformations["type"] == "add_fields":
            lines = process_transformation_add_fields(transformations, lines, input_df)

    data["lines"] = lines
    return data


@task()
def generate_sinks_code(data):
    dataflow = data["dataflow"]
    lines = data["lines"]
    for sink in dataflow["sinks"]:
        input_df = sink["input"]
        name = sink["name"].replace("-", "_")
        if sink["format"] == "KAFKA":
            for topic in sink["topics"]:
                lines += [F"{name}={input_df}.selectExpr('\"{topic}\" as topic','CAST(name AS STRING) as key'"
                          + ", 'CAST(to_json(struct(*)) AS STRING) as value')"]
                lines += [F"print(\"###\"+str({name}.count()))"]
                lines += [F"{name}.write.format('kafka')"
                          + ".option('kafka.bootstrap.servers', 'kafka1:9092,kafka2:9093')"
                          + ".save()"]

        elif sink["format"] in ["JSON", "PARQUET", "CSV"]:
            path = sink["paths"][0]
            lines += [F"{input_df}.write.mode('{sink['saveMode']}')"
                      + F".format('{sink['format']}').save('hdfs://namenode:9000{path}')"]

    data["lines"] = lines
    return data


@task()
def generate_input_code(data):
    source = data["source"]
    lines = [F"{source['name']}=spark.read.option('multiline', 'true')"
             + F".format('{source['format']}').load('hdfs://namenode:9000{source['path']}')"]
    data["lines"] = lines
    return data


@task()
def generate_spark_file(data, tmp_path="/tmp/"):
    lines = data["lines"]
    file_name = data["source"]["name"]
    file_path = tmp_path + file_name + ".py"
    lines = [line+'\n' for line in lines]
    # read the template
    with open(spark_template) as file:
        spark_template_lines = file.readlines()
    lines=spark_template_lines + lines
    logging.info("\n### INI SPARK CODE ###\n"+"".join(lines)+"### END SPARK CODE ###\n")
    if os.path.exists(file_path):
        os.remove(file_path)
    with open(file_path, "a") as file_object:
        file_object.writelines(lines)
    data["file_path"] = file_path
    return data


class CustomSparkSubmitOperator(SparkSubmitOperator):
    def execute(self, context):
        self._application = context["ti"].xcom_pull(task_ids="generate_spark_file")["file_path"]
        super().execute(context)
        return self._hook._spark_exit_code


def check_exit_code(spark_task_id):
    context = get_current_context()
    exit_code = context['ti'].xcom_pull(task_ids=spark_task_id)
    if exit_code in ['1', '125']:  # Place the codes here
        return False
    return True


def iterate_sources(dataflow):
    join = DummyOperator(
        task_id="join",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    for i,source in enumerate(dataflow["sources"]):
        cleaned_source_name = source["name"].replace('-', '_').replace(' ', '_')
        spark_task_id = F"spark_job{'_'+str(i) if i>0 else ''}"
        check_exit_code_id= F"check_exit_code{'_'+str(i) if i>0 else ''}"
        data = {"dataflow": dataflow, "name": dataflow["name"], "source": source}

        data = generate_input_code(data)
        data = generate_transformations_code(data)
        data = generate_sinks_code(data)
        data = generate_spark_file(data)

        data >> CustomSparkSubmitOperator(
            application="default",
            name=cleaned_source_name,
            conn_id=conn_id,
            packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1",
            task_id=spark_task_id,
            conf={"spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp"}
        ) >> ShortCircuitOperator(task_id=check_exit_code_id,
                                  python_callable=lambda: check_exit_code(spark_task_id)) >> join


###########################################
conn_id = "spark_docker"
create_spark_connection_if_not_exists(conn_id=conn_id)

spark_template = '/opt/airflow/include/spark_template.py'

dataflow={'name': 'prueba-acceso', 'sources': [{'name': 'person_inputs', 'path': '/data/input/events/person/*', 'format': 'JSON'}], 'transformations': [{'name': 'validation', 'type': 'validate_fields', 'params': {'input': 'person_inputs', 'validations': [{'field': 'office', 'validations': ['notEmpty']}, {'field': 'age', 'validations': ['notNull']}]}}, {'name': 'ok_with_date', 'type': 'add_fields', 'params': {'input': 'validation_ok', 'addFields': [{'name': 'dt', 'function': 'current_timestamp'}]}}], 'sinks': [{'input': 'ok_with_date', 'name': 'raw-ok', 'topics': ['person'], 'format': 'KAFKA'}, {'input': 'validation_ko', 'name': 'raw-ko', 'paths': ['/data/output/discards/person'], 'format': 'JSON', 'saveMode': 'OVERWRITE'}]}

@dag(dag_id=dataflow["name"],    
     schedule_interval=None,    
     start_date=datetime(2022, 2, 26),    
     catchup=False,    
     tags=['sdg'])    
def taskflow():    
     iterate_sources(dataflow)    

a=taskflow()