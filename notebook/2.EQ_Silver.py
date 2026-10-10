from pyspark.sql.types import (
    StringType, DoubleType, IntegerType, LongType,
    StructField, StructType, ArrayType
)


from pyspark.sql.functions import (
    col, from_json, when, expr,from_unixtime,to_date
)

from delta.tables import DeltaTable

# Geometry schema
geometry_schema = StructType([
    StructField("coordinates", ArrayType(DoubleType()), True)
])

# Properties schema
properties_schema = StructType([
    StructField("mag", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", LongType(), True),
    StructField("tsunami", IntegerType(), True),
    StructField("alert", StringType(), True)
])

# Read Bronze stream
silver_df = (
    spark.readStream
    .table("workspace.earthquake.bronze")
    .withColumn(
        "geometry_parsed",
        from_json(col("geometry"), geometry_schema)
    )
    .withColumn(
        "properties_parsed",
        from_json(col("properties"), properties_schema)
    )
)

# Flatten data
df = (
    silver_df
    .select(
        "id",
        "type",
        col("geometry_parsed.coordinates")[0].alias("longitude"),
        col("geometry_parsed.coordinates")[1].alias("latitude"),
        col("geometry_parsed.coordinates")[2].alias("depth"),
        col("properties_parsed.mag").alias("magnitude"),
        col("properties_parsed.place").alias("place"),
        col("properties_parsed.time").alias("time"),
        col("properties_parsed.tsunami").alias("tsunami"),
        col("properties_parsed.alert").alias("alert"),
        col("ingestion_time"),
        col("path")
    )
)

# Create spatial point with SRID 4326
point = (
    df.withColumn(
        "point",
        expr(
            "st_setsrid(st_point(longitude, latitude), 4326)"
        )
    )
)

# Country boundaries
country_boundaries = spark.read.table(
    "workspace.earthquake.countries_geo"
)

# Spatial join
earthquake_with_country = (
    point.alias("e")
    .join(
        country_boundaries.alias("c"),
        expr(
            "st_contains(c.country_geometry, e.point)"
        ),
        "left"
    )
    .select(
        col("e.*"),
        col("c.country").alias("country"),
        col("c.continent").alias("continent")
    )
)

# Handle null values
final_silver = (
    earthquake_with_country
    .withColumn(
        "country",
        when(
            col("country").isNull(),
            "International Waters"
        ).otherwise(col("country"))
    )
    .withColumn(
        "continent",
        when(
            col("continent").isNull(),
            "Unknown"
        ).otherwise(col("continent"))
    ).withColumn("event_date",to_date(from_unixtime(col("time") / 1000)))
)

def merge_to_silver(batch_df, batch_id):
    silver_table=DeltaTable.forName(batch_df.sparkSession,"workspace.earthquake.silver")
    silver_table.alias("t").merge(batch_df.alias("s"),"t.id=s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    

# Write Silver table
silver_query = (
    final_silver.writeStream
    .trigger(availableNow=True)
    .option(
        "checkpointLocation",
        "/Volumes/workspace/earthquake/checkpoint/silver/"
    )
    .foreachBatch(merge_to_silver)
    .start("workspace.earthquake.silver")
)
