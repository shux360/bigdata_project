import os
from pyspark.sql import SparkSession, functions as F, types as T

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
JDBC = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','postgres')}:5432/smartgrid"
PROPS = {"user": "smartgrid", "password": "smartgrid", "driver": "org.postgresql.Driver"}

meter_schema = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("meter_id", T.StringType()),
    T.StructField("household_id", T.StringType()), T.StructField("power_consumption_kwh", T.DoubleType()),
    T.StructField("solar_generation_kwh", T.DoubleType()), T.StructField("grid_zone", T.StringType()),
    T.StructField("timestamp", T.StringType())])

def transform_meters(df):
    return (df.select(F.from_json(F.col("value").cast("string"), meter_schema).alias("e"))
        .select("e.*").withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_id").isNotNull() & (F.col("power_consumption_kwh") >= 0) & (F.col("solar_generation_kwh") >= 0))
        .withColumn("net_grid_kwh", F.greatest(F.lit(0.0), F.col("power_consumption_kwh") - F.col("solar_generation_kwh"))))

def write_batch(batch, _batch_id):
    if batch.rdd.isEmpty(): return
    clean = batch.dropDuplicates(["event_id"])
    clean.select("event_id","meter_id","household_id","grid_zone","power_consumption_kwh","solar_generation_kwh","net_grid_kwh","event_time").write.jdbc(JDBC,"meter_readings",mode="append",properties=PROPS)
    metrics = (clean.groupBy(F.window("event_time", "1 minute"), "grid_zone")
        .agg(F.sum("power_consumption_kwh").alias("consumption_kwh"), F.sum("solar_generation_kwh").alias("solar_kwh"), F.sum("net_grid_kwh").alias("net_grid_kwh"), F.countDistinct("household_id").alias("active_households"))
        .withColumn("renewable_pct", F.when(F.col("consumption_kwh") > 0, 100*F.col("solar_kwh")/F.col("consumption_kwh")).otherwise(0))
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "grid_zone","consumption_kwh","solar_kwh","net_grid_kwh","active_households","renewable_pct"))
    metrics.write.jdbc(JDBC,"zone_metrics",mode="append",properties=PROPS)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("smart-grid-kappa").getOrCreate(); spark.sparkContext.setLogLevel("WARN")
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers",KAFKA).option("subscribe","meter-readings").option("startingOffsets","earliest").load())
    query = (transform_meters(raw).withWatermark("event_time","2 minutes").writeStream.foreachBatch(write_batch).option("checkpointLocation","/opt/project/checkpoints/meters").trigger(processingTime="10 seconds").start())
    query.awaitTermination()

