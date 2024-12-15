kafka_address = "192.168.1.105:9092"
kafka_topics = ["dataset1", "dataset2", "dataset3"]
kafka_connection_attempts = 10
kafka_connection_attempts_delay = 60
kafka_topic_processed = "processed_messages"
spark_master_address = "spark://192.168.1.109:7077"
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.2.3"
