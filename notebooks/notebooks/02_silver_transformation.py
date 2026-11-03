from pyspark.sql.functions import col, when, lit

silver_df = spark.table(
    "bigmart_catalog.bigmark_schema.bronze_bigmart_sales"
)

display(silver_df)
silver_df = spark.table(
    "bigmart_catalog.bigmark_schema.bronze_bigmart_sales"
)

display(silver_df)

print("Total Records:", silver_df.count())
from pyspark.sql.functions import col, when

silver_df = silver_df.withColumn(
    "Item_Fat_Content",
    when(
        col("Item_Fat_Content").isin("LF", "low fat"),
        "Low Fat"
    ).when(
        col("Item_Fat_Content") == "reg",
        "Regular"
    ).otherwise(
        col("Item_Fat_Content")
    )
)

display(
    silver_df.select("Item_Fat_Content").distinct()
)
silver_df.filter(
    col("Item_Weight").isNull()
).count()
median_weight = silver_df.approxQuantile(
    "Item_Weight",
    [0.5],
    0.01
)

print("Median Item Weight:", median_weight[0])
from pyspark.sql.functions import col,when,lit 

silver_df = silver_df.withColumn(
    "Item_weight",
    when(
        col("Item_Weight").isNull(),
        lit(median_weight[0])
    ).otherwise(
        col("Item_Weight")
    )
)
silver_df.filter(
    col("Item_weight").isNull()
).count()
silver_df.filter(
    col("Outlet_Size").isNull()
).count()
silver_df = silver_df.withColumn(
    "Outlet_Size",
    when(
        col("Outlet_Size").isNull(),
        lit("Unknown")
    ).otherwise(
        col("Outlet_Size")
    )
)
silver_df.groupBy("Outlet_Size").count().show()
from pyspark.sql.functions import col, when, sum as spark_sum

null_counts = silver_df.select(
    [
        spark_sum(
            when(col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in silver_df.columns
    ]
)

display(null_counts)
total_records = silver_df.count()

distinct_records = silver_df.distinct().count()

duplicate_records = total_records - distinct_records 

print("Total Records:", total_records)
print("Distinct Records:", distinct_records)
print("Duplicate Records:", duplicate_records)


bronze_count = spark.table(
    "bigmart_catalog.bigmark_schema.bronze_bigmart_sales"
).count()

silver_count = silver_df.count()

print("Bronze Records:", bronze_count)
print("Silver Records:", silver_count)
print("Records Difference:", bronze_count - silver_count)
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "bigmart_catalog.bigmark_schema.silver_bigmart_sales"
    )
final_silver_df = spark.table(
    "bigmart_catalog.bigmark_schema.silver_bigmart_sales"
)

print("Final Silver Records:", final_silver_df.count)

display(final_silver_df)
