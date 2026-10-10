from delta.tables import DeltaTable

from pyspark.sql.functions import (
    col,
    count,
    sum,
    min,
    max,
    to_date,
    from_unixtime
)

# ==========================================
# Gold Merge Function
# ==========================================

def update_gold(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    batch_agg = (
        batch_df
        .withColumn(
            "event_date",
            to_date(
                from_unixtime(col("time") / 1000)
            )
        )
        .groupBy("event_date")
        .agg(
            count("*").alias("new_count"),
            sum("magnitude").alias("new_sum_magnitude"),
            max("magnitude").alias("new_max_magnitude"),
            min("magnitude").alias("new_min_magnitude"),
            sum("depth").alias("new_sum_depth"),
            sum(
                col("tsunami").cast("int")
            ).alias("new_tsunami")
        )
    )

    gold_table = DeltaTable.forName(
        spark,
        "workspace.earthquake.gold"
    )

    (
        gold_table.alias("t")
        .merge(
            batch_agg.alias("s"),
            "t.event_date = s.event_date"
        )
        .whenMatchedUpdate(
            set={
                "total_earthquakes":
                    "t.total_earthquakes + s.new_count",

                "sum_magnitude":
                    "t.sum_magnitude + s.new_sum_magnitude",

                "maximum_magnitude":
                    "greatest(t.maximum_magnitude, s.new_max_magnitude)",

                "minimum_magnitude":
                    "least(t.minimum_magnitude, s.new_min_magnitude)",

                "sum_depth":
                    "t.sum_depth + s.new_sum_depth",

                "total_tsunami":
                    "t.total_tsunami + s.new_tsunami"
            }
        )
        .whenNotMatchedInsert(
            values={
                "event_date":
                    "s.event_date",

                "total_earthquakes":
                    "s.new_count",

                "sum_magnitude":
                    "s.new_sum_magnitude",

                "maximum_magnitude":
                    "s.new_max_magnitude",

                "minimum_magnitude":
                    "s.new_min_magnitude",

                "sum_depth":
                    "s.new_sum_depth",

                "total_tsunami":
                    "s.new_tsunami"
            }
        )
        .execute()
    )


# ==========================================
# Gold Stream
# ==========================================

gold_query = (
    spark.readStream
    .table("workspace.earthquake.silver")
    .writeStream
    .foreachBatch(update_gold)
    .option(
        "checkpointLocation",
        "/Volumes/workspace/earthquake/checkpoint/gold/"
    )
    .trigger(availableNow=True)
    .start()
)
