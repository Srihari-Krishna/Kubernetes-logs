# Kubernetes-logs

kubectl get pods -n kube-system

kubectl logs -n kube-system -l k8s-app=fluent-bit


kubectl rollout restart daemonset/fluent-bit -n kube-system



$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --jars postgresql-42.7.3.jar kafka_spark_stream.py

spark-submit --jars postgresql-42.7.3.jar batch.py


docker exec -it kafka bash
docker exec -it dbt_postgres_1 psql -U sparkuser -d sparklogs


kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-console-consumer.sh --bootstrap-server 172.17.0.1:9092 --topic kube-logs --from-beginning

kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic <topic-name>

docker-compose up -d
docker-compose -f docker-compose.postgres.yml up -d


docker-compose down --volumes --remove-orphans
docker system prune -f --volumes


docker exec -it kafka kafka-topics.sh --create --topic kube-logs-info --bootstrap-server localhost:9092

minikube service nginx

kubectl rollout restart deployment fake-logger

docker build -t sriharik22/fake-logger:latest .

docker push sriharik22/fake-logger:latest
