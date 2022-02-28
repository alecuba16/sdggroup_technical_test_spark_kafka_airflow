import json
import os
import shutil

cd = os.path.dirname(os.path.realpath(__file__))
template_filepath = F"{cd}/../input_files/sdg_template.json"
dag_template = F"{cd}/dag_template.py"
output_path = F"{cd}/../airflow/dags/"

f = open(template_filepath)
config = json.load(f)
f.close()

for dataflow in config["dataflows"]:
    file_path=output_path+dataflow["name"].replace('-','_').replace(' ','_')+".py"
    if os.path.exists(file_path):
        os.remove(file_path)
    shutil.copyfile(dag_template, file_path)
    dag_generator=[F"\ndataflow={dataflow}"]
    dag_generator+=[F"\n@dag(dag_id=dataflow[\"name\"],\
    \n\tschedule_interval=None,\
    \n\tstart_date=datetime(2022, 2, 26),\
    \n\tcatchup=False,\
    \n\ttags=['sdg'])\
    \ndef taskflow():\
    \n    iterate_sources(dataflow)\
    \na=taskflow()"]
    with open(file_path, "a") as file_object:
        file_object.writelines(dag_generator)