from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

TOPIC_IN = 'transaction-service-input'

spark_master = 'local'
spark_app_name = "TransactionsKafkaToPostgresStager"

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

postgresql_settings = {
    'user': 'jovyan',
    'password': 'jovyan',
    'driver': 'org.postgresql.Driver',
}

kafka_security_options_ssl = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="producer_consumer" password="sprint_11";',
}
kafka_bootstrap_servers_ssl = 'rc1a-sd5jrikpd9jcve1c.mdb.yandexcloud.net:9091'

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .master(spark_master) \
    .appName(spark_app_name) \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с транзакциями и валютами
read_stream_df = spark.readStream \
    .format('kafka') \
    .options(**kafka_security_options_ssl) \
    .option('kafka.bootstrap.servers', kafka_bootstrap_servers_ssl) \
    .option('subscribe', TOPIC_IN) \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("object_id", StringType(), nullable=True),
    StructField("object_type", StringType(), nullable=True),
    StructField("sent_dttm", TimestampType(), nullable=True),
    StructField("payload", StringType(), nullable=True),
])

# десериализуем из value сообщения json
deserialized_df = read_stream_df \
        .withColumn('key_str', col('key').cast(StringType())) \
        .withColumn('value_json', col('value').cast(StringType())) \
        .drop('key', 'value') \
        .withColumn('key', col('key_str')) \
        .withColumn('value', from_json(col('value_json'), incomming_message_schema)) \
        .drop('key_str', 'value_json') \
        .select(
            col('value.object_id').alias('object_id'),
            col('value.object_type').alias('object_type'),
            col('value.sent_dttm').alias('sent_dttm'),
            col('value.payload').alias('payload'),
        ) \
        .dropDuplicates(['object_type', 'object_id']) \
        .withWatermark('sent_dttm', '5 minutes')


# метод для записи данных в 2 target, оба в PostgreSQL
def foreach_batch_function(df, epoch_id):
    # персистим df, так как нам от него фильтроваться как минимум два раза
    df.persist()

    # currencies
    # static df, so write()
    df_postgres_currencies = df.filter(
        col("object_type") == 'CURRENCY'
    )
    df_postgres_currencies \
        .write \
        .jdbc(url='jdbc:postgresql://localhost:5432/de?currentSchema=stg',
              table="currencies", mode="append", properties=postgresql_settings)

    # transactions
    # static df, so write()
    df_postgres_transactions = df.filter(
        col("object_type") == 'TRANSACTION'
    )
    df_postgres_transactions \
        .write \
        .jdbc(url='jdbc:postgresql://localhost:5432/de?currentSchema=stg',
              table="transactions", mode="append", properties=postgresql_settings)

    # очищаем память от df
    df.unpersist()


# запускаем стриминг
deserialized_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
