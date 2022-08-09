"""
Databricks File System"

- DBFS는 데이타브릭스에서 사용하는 file system이다.
- DBFS의 파일은 Blob 저장소에 유지되므로 클러스터가 종료된 후에도 데이터가 손실되지 않는다.
"""

"""
- Databricks Utilities 클래스(및 기타 파일 IO 루틴)를 통해 DBFS에 액세스합니다.
- DBUtils의 인스턴스는 이미 dbutils로 선언되어 있습니다.
- DBUtils에 대한 노트북 내 문서의 경우 dbutils.help() 명령을 실행하십시오.
- https://docs.databricks.com/dev-tools/databricks-utils.html 참조
"""

dbutils.help()


"""
Reading from CSV

- products에서 데이터 불러오는 예제
- %fs head를 사용하여 데이터 확인 가능

%fs head /mnt/training/initech/products/dirty.csv
"""

CSV_FILE = "dbfs:/mnt/training/initech/products/dirty.csv"
tempDF = spark.read.csv(CSV_FILE)

tempDF.printSchema()
display(tempDF)



"""Use the File Header(헤더 포함)"""
(spark.read                    # The DataFrameReader
   .option("header", True)     # Use first line of all files as header
   .csv(CSV_FILE)              # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)

"""Infer the Schema(스키마 추론)"""
(spark.read
   .option("header", True)
   .option("inferSchema", True)    # Automatically infer data types
   .csv(CSV_FILE)
   .printSchema()
)

"""Declare the schema.(스키마 정의해서 적용)"""
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

csvSchema = StructType([
  StructField("product_id", IntegerType()),
  StructField("category", StringType()),
  StructField("brand", StringType()),
  StructField("model", StringType()),
  StructField("price", DoubleType()),
  StructField("processor", StringType()),
  StructField("size", StringType()),
  StructField("display", StringType())
 ])

"""Read the data (and print the schema)."""
productDF = (spark.read
  .option('header', 'true')
  .schema(csvSchema)          # Use the specified schema
  .csv(CSV_FILE)
)
"""sql 사용"""
productDF.createOrReplaceTempView("products")
# %sql
SELECT * FROM products

"""Writing to Parquet(parquet로 쓰기)"""
OUTPUT_FILE = userhome + "/initech/Products.parquet"
productDF.write.mode("overwrite").parquet(OUTPUT_FILE)

