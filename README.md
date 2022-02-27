# sdggroup_technical_test_spark_kafka_airflow
spark + airflow + kafka dynamic graph generation


## Build because I had to add JAVA to the airflow image, because its required by spark_submit
docker-compose up --build

# Upload person data (!required)
./upload_person_input.sh

### Utils 
docker-compose ps

#### Remove old images
docker rmi -f $(docker images -a -q)
#### Purge images,etc
docker container prune -f && docker image prune -f && docker volume prune -f
