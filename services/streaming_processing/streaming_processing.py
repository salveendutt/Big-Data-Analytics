from utils_streaming_processing import (
    create_spark_session,
    kafka_schema,
    kafka_stream_subscription,
    process_kafka_stream,
    send_to_kafka_stream,
)


if __name__ == "__main__":
    spark = create_spark_session()
    schema = kafka_schema()
    kafka_stream = kafka_stream_subscription(spark)
    processed_stream = process_kafka_stream(kafka_stream, schema)
    send_to_kafka_stream(processed_stream)
