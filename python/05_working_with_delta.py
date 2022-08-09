inputPath = "/mnt/training/online_retail/data-001/data.csv"
DataPath = userhome + "/delta/customer-data/"

dbutils.fs.rm(DataPath, True)

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

inputSchema = StructType([
  StructField("InvoiceNo", IntegerType(), True),
  StructField("StockCode", StringType(), True),
  StructField("Description", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("InvoiceDate", StringType(), True),
  StructField("UnitPrice", DoubleType(), True),
  StructField("CustomerID", IntegerType(), True),
  StructField("Country", StringType(), True)
])

rawDataDF = (spark.read
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath)
)

# write to databricks delta 
# format("delta")으로 정의
rawDataDF.write.mode("overwrite")\
    .format("delta")\
    .partitionBy("Country")\
    .save(DataPath)


spark.sql("""
  DROP TABLE IF EXISTS customer_data_delta
""")
spark.sql("""
  CREATE TABLE customer_data_delta
  USING DELTA
  LOCATION '{}'
""".format(DataPath))


# append 
miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

newDataDF = (spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)
)

(newDataDF
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("append")
  .save(DataPath)
)

miniDataPath = userhome + "/delta/customer-data-mini/"
miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

miniDataDF = (spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)
)

(miniDataDF
  .write
  .mode("overwrite")
  .format("delta")
  .save(miniDataPath)
)

spark.sql("""
    DROP TABLE IF EXISTS customer_data_delta_mini
  """)
spark.sql("""
    CREATE TABLE customer_data_delta_mini
    USING DELTA
    LOCATION '{}'
  """.format(miniDataPath))


from pyspark.sql.functions import lit, col
customerSpecificDF = (miniDataDF
  .filter("CustomerID=20993")
  .withColumn("StockCode", lit(99999))
  .withColumn("InvoiceNo", col("InvoiceNo").cast("String"))
 )

spark.sql("DROP TABLE IF EXISTS customer_data_delta_to_upsert")
customerSpecificDF.write.saveAsTable("customer_data_delta_to_upsert")


"""
새 데이터를 customer_data_delta_mini에 업삽트하십시오.

Upsert는 MERGE INTO 구문을 사용하여 수행됩니다.

%sql
MERGE INTO customer_data_delta_mini
USING customer_data_delta_to_upsert
ON customer_data_delta_mini.CustomerID = customer_data_delta_to_upsert.CustomerID
WHEN MATCHED THEN
  UPDATE SET
    customer_data_delta_mini.StockCode = customer_data_delta_to_upsert.StockCode
WHEN NOT MATCHED
  THEN INSERT (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
  VALUES (
    customer_data_delta_to_upsert.InvoiceNo,
    customer_data_delta_to_upsert.StockCode,
    customer_data_delta_to_upsert.Description,
    customer_data_delta_to_upsert.Quantity,
    customer_data_delta_to_upsert.InvoiceDate,
    customer_data_delta_to_upsert.UnitPrice,
    customer_data_delta_to_upsert.CustomerID,
    customer_data_delta_to_upsert.Country)
"""
