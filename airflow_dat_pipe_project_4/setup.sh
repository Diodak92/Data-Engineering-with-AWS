folder_name=airflow-docker
mkdir $folder_name
cd $folder_name
curl -LfO https://airflow.apache.org/docs/apache-airflow/2.10.2/docker-compose.yaml
echo "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up