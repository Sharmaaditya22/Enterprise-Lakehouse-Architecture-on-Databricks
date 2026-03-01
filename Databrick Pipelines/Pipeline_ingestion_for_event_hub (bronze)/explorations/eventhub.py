# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

EH_NAMESPACE=spark.conf.get('eh_namespace')
EH_NAME=spark.conf.get('eh_name')
EH_CONN_STR=spark.conf.get('eh_connectionstring')

# COMMAND ----------

# DBTITLE 1,Cell 2
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "maxOffsetsPerTrigger": "50000",
    "failOnDataLoss": "true",
    "startingOffsets": "earliest",
}

# COMMAND ----------

# DBTITLE 1,Cell 3
df_raw=spark.readStream.format('Kafka')\
    .options(**KAFKA_OPTIONS)\
    .load()

df_raw.display()

# COMMAND ----------

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("items", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Cell 5
df_parsed=df_raw.withColumn('key_str',col('key').cast('string'))\
    .withColumn('value_str',col('value').cast('string'))\
    .withColumn('data',from_json(col('value_str'),schema))\
    .select('data.*')\
    .withColumnRenamed('timestamp','order_timestamp')
df_parsed.display()

# COMMAND ----------

