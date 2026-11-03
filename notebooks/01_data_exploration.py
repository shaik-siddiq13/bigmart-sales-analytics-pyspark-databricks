print("BigMart Sales Analytics Project")

print("Spark Version:", spark.version)

print("Databricks connection successful!")
file_path = "/Volumes/bigmart_catalog/bigmark_schema/bigmart_volume/BigMart Sales.csv"

df = spark.read.format("csv") \
    .option("header",True) \
    .option("inferSchema",True) \
    .load(file_path)

display(df)
print("Total Records:", df.count())

print("Total Columns:", len(df.columns))

print("Column names:",df.columns)
df.printSchema()
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bigmart_catalog.bigmark_schema.bronze_bigmart_sales")
bronze_df = spark.table("bigmart_catalog.bigmark_schema.bronze_bigmart_sales")

display(bronze_df)
print("Bronze Table Records:",bronze_df.count())
%sql
select * from bigmart_catalog.bigmark_schema.bronze_bigmart_sales
limit 10;
%sql
DESCRIBE EXTENDED bigmart_catalog.bigmark_schema.bronze_bigmart_sales;
%sql
DESCRIBE HISTORY bigmart_catalog.bigmark_schema.bronze_bigmart_sales;
from pyspark.sql.functions import col,when,sum as spark_sum

df = spark.table("bigmart_catalog.bigmark_schema.bronze_bigmart_sales")

null_counts = df.select(
    [
        spark_sum(
            when(col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in df.columns
    ]
)

display(null_counts)
from pyspark.sql.functions import col, when, sum as spark_sum, round

total_records = df.count()

null_percentage = df.select(
    [
        round(
            spark_sum(
                when(col(c).isNull(), 1).otherwise(0)
            ) / total_records * 100,
            2
        ).alias(c)
        for c in df.columns
    ]
)

display(null_percentage)
total_records = df.count()

distinct_records = df.distinct().count()

duplicate_records = total_records - distinct_records

print("Total Records:",total_records)
print("Distinct Records:", distinct_records)
print("Duplicate Records:", duplicate_records)
df.select("Item_Fat_Content").distinct().show()
from pyspark.sql.functions import col, when

df_clean = df.withColumn(
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

display(df_clean.select("Item_Fat_Content").distinct())
print("original Categories:")
df.select("Item_Fat_Content").distinct().show()
print("Cleaned Categories:")
df_clean.select("Item_Fat_Content").distinct().show()
from pyspark.sql.functions import col 

missing_weight_df = df_clean.filter(
    col("Item_Weight").isNull()
)

display(missing_weight_df)
df_clean.select("Item_Weight").describe().show()
median_weight = df_clean.approxQuantile(
    "Item_Weight",
    [0.5],
    0.01
)

print("Median Item Weight:",median_weight[0])
from pyspark.sql.functions import col,when,lit

median_weight = 12.5

df_clean = df_clean.withColumn(
    "Item_weight",
    when(
        col("Item_Weight").isNull(),
        lit(median_weight)
    ).otherwise(
        col("Item_Weight")
    )
)
df_clean.filter(
    col("Item_weight").isNull()
).count()
df_clean.select("outlet_size").distinct().show()
df_clean.groupBy("Outlet_Size").count().show()
df_clean.filter(
    col("Outlet_Size").isNull()
).select(
    "Outlet_Identifier",
    "Outlet_Location_Type",
    "Outlet_Type",
    "Outlet_Size"
).distinct().show(50,truncate=False)
df_clean.groupBy(
    "Outlet_Identifier",
    "Outlet_Location_Type",
    "Outlet_Type",
    "Outlet_Size"
).count().orderBy(
    "Outlet_Identifier"
).show(50, truncate=False)
df_clean.filter(
    col("Outlet_size").isNull()
).count()
from pyspark.sql.functions import col,when,lit

df_clean = df_clean.withColumn(
    "Outlet_size",
    when(
        col("Outlet_size").isNull(),
        lit("unknown")
    ).otherwise(
        col("Outlet_Size")
    )
)
df_clean.groupBy("Outlet_size").count().show()
from pyspark.sql.functions import col,when,sum as spark_sum 

null_counts = df_clean.select(
    [
        spark_sum(
            when(col(c).isNull(),1).otherwise(0)
        ).alias(c)
        for c in df_clean.columns
    ]
)

display(null_counts)
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "bigmart_catalog.bigmark_schema.silver_bigmart_sales"
    )
silver_df = spark.table(
    "bigmart_catalog.bigmark_schema.silver_bigmart_sales"
)

display(silver_df)

print("Silver Records:", silver_df.count())
silver_df.filter(
    col("Item_Weight").isNull()
).count()
