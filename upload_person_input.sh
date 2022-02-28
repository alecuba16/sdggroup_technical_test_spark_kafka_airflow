#!/bin/bash
#docker exec -it namenode bash
docker exec namenode hdfs dfs -rm -r /data/input/events/person
docker exec namenode hdfs dfs -mkdir -p /data/input/events/person
docker exec namenode hdfs dfs -rm -r /data/output/discards
docker exec namenode hdfs dfs -mkdir -p /data/output/discards
docker exec namenode hdfs dfs -put /tmp/person_inputs.json /data/input/events/person/person_inputs.json
