# streaming_watt_sn.py
# ==========================================================
# Watt-SN - Spark Structured Streaming
# Consommation électrique + Production solaire
# Classification des risques de délestage
# ==========================================================

from pyspark.sql.functions import to_timestamp
import os


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
   col,
   sha2,
   concat,
   lit,
   from_json,
   to_json,
   struct,
   window,
   avg,
   sum as spark_sum,
   when,
   coalesce,
   current_timestamp
)


from pyspark.sql.types import (
   StructType,
   StructField,
   StringType,
   FloatType,
   IntegerType
)


# ==========================================================
# CONFIGURATION
# ==========================================================


SALT = os.environ.get(
   "WATT_SECRET_SALT",
   "watt_sn_secret"
)


# Kafka dans Docker
BROKERS = "kafka:29092"


# ==========================================================
# SESSION SPARK
# ==========================================================


spark = (
   SparkSession.builder
   .appName("Watt_SN_Streaming")
   .config("spark.sql.shuffle.partitions", "4")
   .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")


# ==========================================================
# SCHEMA DONNEES CONSOMMATION
# ==========================================================


conso_schema = StructType([
   StructField("compteur_id", StringType(), True),
   StructField("zone", StringType(), True),
   StructField("consommation_mw", FloatType(), True),
   StructField("heure", IntegerType(), True),
   StructField("timestamp", StringType(), True)
])


# ==========================================================
# SCHEMA DONNEES SOLAIRES
# ==========================================================


solaire_schema = StructType([
   StructField("zone", StringType(), True),
   StructField("production_mw", FloatType(), True),
   StructField("capacite_installee_mw", FloatType(), True),
   StructField("heure", IntegerType(), True),
   StructField("timestamp", StringType(), True)
])


# ==========================================================
# ETAPE 1
# LECTURE KAFKA - CONSOMMATION
# ==========================================================


conso_df = (
   spark.readStream
   .format("kafka")
   .option("kafka.bootstrap.servers", BROKERS)
   .option("subscribe", "watt_conso_raw")
   .option("failOnDataLoss", "false")
   .option("startingOffsets", "latest")
   .load()
   .select(
       from_json(
           col("value").cast("string"),
           conso_schema
       ).alias("data")
   )
   .select("data.*")
)


# ==========================================================
# ETAPE 2
# AJOUT TIMESTAMP EVENEMENT
# ==========================================================


conso_df = (
#    conso_df
#    .withColumn(
#        "event_ts",
#        current_timestamp()
#    )
    conso_df
    .withColumn(
        "event_ts",
        to_timestamp("timestamp")
    )
)


# ==========================================================
# ETAPE 3
# ANONYMISATION SHA-256
# ==========================================================


conso_df = (
   conso_df
   .withColumn(
       "compteur_secure",
       sha2(
           concat(
               col("compteur_id"),
               lit(SALT)
           ),
           256
       )
   )
   .drop("compteur_id")
)


# ==========================================================
# LECTURE KAFKA - SOLAIRE
# ==========================================================


solaire_df = (
   spark.readStream
   .format("kafka")
   .option("kafka.bootstrap.servers", BROKERS)
   .option("subscribe", "watt_solaire_raw")
   .option("failOnDataLoss", "false")
   .option("startingOffsets", "latest")
   .load()
   .select(
       from_json(
           col("value").cast("string"),
           solaire_schema
       ).alias("data")
   )
   .select("data.*")
   .withColumn(
       "event_ts",
       to_timestamp("timestamp")
   )
)


# ==========================================================
# ETAPE 4
# WATERMARK + AGRÉGATION CONSOMMATION
# Fenêtre 1h - Slide 15 min
# ==========================================================


conso_agg = (
   conso_df
   .withWatermark(
       "event_ts",
       "10 minutes"
   )
   .groupBy(
       window(
           "event_ts",
           "1 hour",
           "15 minutes"
       ),
       "zone"
   )
   .agg(
       spark_sum("consommation_mw")
       .alias("conso_totale_mw"),


       avg("consommation_mw")
       .alias("conso_moy_mw")
   )
)


# ==========================================================
# AGRÉGATION SOLAIRE
# ==========================================================


solaire_agg = (
   solaire_df
   .withWatermark(
       "event_ts",
       "10 minutes"
   )
   .groupBy(
       window(
           "event_ts",
           "1 hour",
           "15 minutes"
       ),
       "zone"
   )
   .agg(
       spark_sum("production_mw")
       .alias("prod_solaire_mw")
   )
)


# ==========================================================
# ETAPE 5
# CLASSIFICATION RISQUE DELESTAGE
# ==========================================================


risque_df = (
   conso_agg
   .withColumn(
       "risque_delestage",


       when(
           coalesce(
               col("conso_totale_mw"),
               lit(0)
           ) > 10000,
           lit("ROUGE")
       )


       .when(
           coalesce(
               col("conso_totale_mw"),
               lit(0)
           ) > 5000,
           lit("ORANGE")
       )


       .otherwise(
           lit("VERT")
       )
   )
)


# ==========================================================
# ETAPE 6
# ALERTES → KAFKA
# ==========================================================


alertes_kafka = (
   risque_df
   .select(
       to_json(
           struct("*")
       ).alias("value")
   )
)


debug_query = (
   risque_df.writeStream
   .format("console")
   .outputMode("update")
   .start()
)


q_alerts = (
   alertes_kafka
   .writeStream
   .format("kafka")
   .option(
       "kafka.bootstrap.servers",
       BROKERS
   )
   .option(
       "topic",
       "watt_alerts"
   )
   .option(
       "checkpointLocation",
       "/tmp/watt_alerts_ckpt"
   )
   .outputMode("update")
   .start()
)
q_debug_solaire = (
   solaire_agg
   .writeStream
   .format("console")
   .outputMode("update")
   .start()
)


# ==========================================================
# AGRÉGATS SOLAIRES → KAFKA
# ==========================================================


solaire_kafka = (
   solaire_agg
   .select(
       to_json(
           struct("*")
       ).alias("value")
   )
)


q_solaire = (
   solaire_kafka
   .writeStream
   .format("kafka")
   .option(
       "kafka.bootstrap.servers",
       BROKERS
   )
   .option(
       "topic",
       "watt_solaire_agg"
   )
   .option(
       "checkpointLocation",
       "/tmp/watt_solaire_ckpt"
   )
   .outputMode("update")
   .start()
)


# ==========================================================
# DEMARRAGE STREAMING
# ==========================================================


print("Pipeline Spark Streaming Watt-SN démarré...")


# q_alerts.awaitTermination()
# q_solaire.awaitTermination()
# q_debug_solaire.awaitTermination()


spark.streams.awaitAnyTermination()
