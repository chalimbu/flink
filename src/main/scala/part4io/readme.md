got to folder docker/kafka/docker-compose.yml
and run
docker compose up
in another terminal
docker exec -it rockthejvm-flink-broker bash
/bin/kafka-topics --bootstrap-server localhost:9092 --topic events --create

console consumer
docker exec -it rockthejvm-flink-broker bash
/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic events --from-beginning

console producer
docker exec -it rockthejvm-flink-broker bash
/bin/kafka-console-producer --broker-list localhost:9092 --topic events

in KafkaIntegration we use the connector of kafka integraiton

----------------------
same with the other changing topic to people
