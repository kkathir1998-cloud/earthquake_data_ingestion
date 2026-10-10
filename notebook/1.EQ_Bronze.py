
from pyspark.sql.functions import current_timestamp,col
source="/Volumes/workspace/earthquake_live_feed/incoming"
schemalocation="/Volumes/workspace/earthquake/schemalocation/bronze/"
checkpoint="/Volumes/workspace/earthquake/checkpoint/bronze/"
bronze_df=spark.readStream.format("cloudFiles").option("cloudFiles.format","json").option("cloudFiles.schemaLocation",schemalocation).option("cloudFiles.schemaEvolutionMode","rescue").option("rescueDataColumn","bad_data").load(source).withColumn("ingestion_time",current_timestamp()).withColumn("path",col("_metadata.file_path"))
