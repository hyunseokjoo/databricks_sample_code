from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType

file_path = "abfss://{container_name}@{Storage_Account_name}.dfs.core.windows.net/{Path}"
table_name = "{catalog_name}.{database_name}.{table_name}"
file_format = "parquet"
checkpoint_path = "/tmp/stream/"

# 기존 작업이 있다면 아래 명령어를 먼저 실행 
spark.sql(f"Drop table if exists {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# parquet 바이너리 오류 위해 설정 추가
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.parquet.binaryAsString", "true") 

# Schema 생성
schema = StructType([
    StrucnField("No", DecimalType(), True),
    StructField("Num", IntegerType(), True),
    StructField("Time", TimestampType(), True),
    StructField("Name", StringType(), True)
])

# Autoloder 실행 및 데이터 write
from pyspark.sql.functions import input_file_name, current_timestamp

df = (spark.readStream
    .format("parquet")
    .schema(schema)
    .load(file_path)
    .select("No", "Num", "Time", "Name")
    .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True) # 이 옵션은 한번만 실행 하도록 설정하는 것으로 운영에서는 주석처리 요망
    .toTable(table_name)
)

# source 갯수 카운트 
df_source = spark.read.parquet(file_path)
df_source.count()

# target 테이블 갯수 카운트
# %sql
# selece count(*) from {catalog_name}.{database_name}.{table_name}


