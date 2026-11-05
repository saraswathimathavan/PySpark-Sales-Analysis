
# Apache Spark Sales Report using AWS Glue

# 1. Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

# 2. Initialize SparkSession
spark = SparkSession.builder \
    .appName("SalesRegionalReport") \
    .getOrCreate()

print("SparkSession initialized successfully!")

# 3. Define S3 input and output paths
input_path = "s3://sales-ishu/input/sales_data.csv"
output_path = "s3://sales-ishu/output/"

# 4. Load CSV file into Spark DataFrame
sales_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("CSV file loaded successfully!")

# Display original sales data
sales_df.show()

# Display schema
sales_df.printSchema()

# 5. Filter transactions where amount is greater than zero
# Transformation: Lazy evaluation
filtered_df = sales_df.filter(col("amount") > 0)

# 6. Count filtered transactions
# Action: Triggers Spark execution
filtered_count = filtered_df.count()

print("Transactions after filtering:", filtered_count)

# Display filtered data
filtered_df.show()

# 7. Aggregate total sales by region
regional_sales = filtered_df.groupBy("region").agg(
    sum("amount").alias("total_sales"),
    count("*").alias("transaction_count")
)

# Sort by region
regional_sales = regional_sales.orderBy("region")

# 8. Display final aggregated results
print("Final Regional Sales Report:")

regional_sales.show(truncate=False)

# 9. Write results to S3 in Parquet format
regional_sales.write \
    .mode("overwrite") \
    .parquet(output_path)

print("Parquet output written successfully!")

# 10. Stop SparkSession
spark.stop()
