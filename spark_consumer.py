import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, when, to_timestamp, datediff 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

transaction_schema = StructType([
    StructField("TransactionID", StringType()),
    StructField("AccountID", StringType()),
    StructField("TransactionAmount", DoubleType()),
    StructField("TransactionDate", StringType()),
    StructField("TransactionType", StringType()),
    StructField("Location", StringType()),
    StructField("DeviceID", StringType()),
    StructField("IP Address", StringType()),
    StructField("MerchantID", StringType()),
    StructField("Channel", StringType()),
    StructField("CustomerAge", IntegerType()),
    StructField("CustomerOccupation", StringType()),
    StructField("TransactionDuration", IntegerType()),
    StructField("LoginAttempts", IntegerType()),
    StructField("AccountBalance", DoubleType()),
    StructField("PreviousTransactionDate", StringType()),
])

spark = SparkSession.builder \
    .appName("BankFraudPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.postgresql:postgresql:42.7.4") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank-transactions") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "50") \
    .load()

parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), transaction_schema).alias("data")) \
    .select("data.*")
fraud_flagged = parsed_stream \
    .withColumn("txn_date", to_timestamp(col("TransactionDate"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("prev_txn_date", to_timestamp(col("PreviousTransactionDate"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("flag_excessive_logins",
        when(col("LoginAttempts") >= 4, 1).otherwise(0)) \
    .withColumn("flag_large_amount_vs_balance",
        when(col("TransactionAmount") > 0.5 * col("AccountBalance"), 1).otherwise(0)) \
    .withColumn("flag_rapid_large_transaction",
        when((col("TransactionDuration") < 10) & (col("TransactionAmount") > 500), 1).otherwise(0)) \
    .withColumn("flag_dormant_reactivation",
        when((datediff(col("txn_date"), col("prev_txn_date")) > 30) & (col("TransactionAmount") > 500), 1).otherwise(0)) \
    .withColumn("fraud_score",
        col("flag_excessive_logins") + col("flag_large_amount_vs_balance") + col("flag_rapid_large_transaction") + col("flag_dormant_reactivation")) \
    .withColumn("is_flagged",
        when(col("fraud_score") >= 1, True).otherwise(False))

POSTGRES_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "bankpipeline",
    "user": "bankuser",
    "password": "bankpass",
}

def write_to_postgres(batch_df, batch_id):
    deduped_df = batch_df.dropDuplicates(["TransactionID"])
    rows = deduped_df.collect()
    print(f"Writing batch {batch_id}: {batch_df.count()} rows received, "
          f"{len(rows)} after in-batch deduplication...")

    if not rows:
        return

    conn = psycopg2.connect(**POSTGRES_CONN_PARAMS)
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO raw_transactions (
            "TransactionID", "AccountID", "TransactionAmount", "TransactionDate",
            "TransactionType", "Location", "DeviceID", "IP Address", "MerchantID",
            "Channel", "CustomerAge", "CustomerOccupation", "TransactionDuration",
            "LoginAttempts", "AccountBalance", "PreviousTransactionDate",
            flag_excessive_logins, flag_large_amount_vs_balance,
            flag_rapid_large_transaction, flag_dormant_reactivation,
            fraud_score, is_flagged
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("TransactionID") DO NOTHING
    """

    values = [
        (
            r["TransactionID"], r["AccountID"], r["TransactionAmount"], r["TransactionDate"],
            r["TransactionType"], r["Location"], r["DeviceID"], r["IP Address"], r["MerchantID"],
            r["Channel"], r["CustomerAge"], r["CustomerOccupation"], r["TransactionDuration"],
            r["LoginAttempts"], r["AccountBalance"], r["PreviousTransactionDate"],
            r["flag_excessive_logins"], r["flag_large_amount_vs_balance"],
            r["flag_rapid_large_transaction"], r["flag_dormant_reactivation"],
            r["fraud_score"], r["is_flagged"],
        )
        for r in rows
    ]

    cur.executemany(insert_sql, values)
    conn.commit()
    cur.close()
    conn.close()

query = fraud_flagged.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

query.awaitTermination()
