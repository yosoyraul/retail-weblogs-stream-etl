Simple ETL pipeline simulating retail weblog streaming using Spark, Kafka, and Cassandra.

Spark submit package dependencies: 
- org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1
- com.datastax.spark:spark-cassandra-connector_2.12:3.0.1

*stream_handler requires --job with appropriate job name to run.

Project was developed using docker to instantiate Kafka and Cassandra services.

Kafka git repository for this project:
https://github.com/conduktor/kafka-stack-docker-compose
*Due to local contraints on resources,the single zookeeper and single kafka service was used for the purpose of this project.

Cassandra was pulled from docker hub following the instructions in the documentation.
https://hub.docker.com/_/cassandra/

The main application to generate logs is the genhttplogsproducer.py file, which takes two arguments being the kafka host and port (e.g. 'localhost:0000') and the topic name. The logs generated are sent directly to the Kafka producer. Kafka not running while application is executed may raise errors.
