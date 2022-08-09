"""
source -> Batch layer -> quries -> BI insights
Streaming Data -> streaming layer -> quries -> BI insights

Batch layer or streaming layer
- Bronze 
- Silver
- Gold
"""
# 단계별로 bronze, silver, gold로 나눠서 프로세싱을 진행함
basePath       = userhome + "/wikipedia-streaming"
bronzePath     = basePath + "/wikipediaEditsRaw.delta"
silverPath     = basePath + "/wikipediaEdits.delta"
goldPath       = basePath + "/wikipediaEditsSummary.delta"
checkpointPath = basePath + "/checkpoints"

# schema정의
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

schema = StructType([
  StructField("channel", StringType(), True),
  StructField("comment", StringType(), True),
  StructField("delta", IntegerType(), True),
  StructField("flag", StringType(), True),
  StructField("geocoding", StructType([                 # (OBJECT): Added by the server, field contains IP address geocoding information for anonymous edit.
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("countryCode2", StringType(), True),
    StructField("countryCode3", StringType(), True),
    StructField("stateProvince", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
  ]), True),
  StructField("isAnonymous", BooleanType(), True),      # (BOOLEAN): Whether or not the change was made by an anonymous user
  StructField("isNewPage", BooleanType(), True),
  StructField("isRobot", BooleanType(), True),
  StructField("isUnpatrolled", BooleanType(), True),
  StructField("namespace", StringType(), True),         # (STRING): Page's namespace. See https://en.wikipedia.org/wiki/Wikipedia:Namespace
  StructField("page", StringType(), True),              # (STRING): Printable name of the page that was edited
  StructField("pageURL", StringType(), True),           # (STRING): URL of the page that was edited
  StructField("timestamp", StringType(), True),         # (STRING): Time the edit occurred, in ISO-8601 format
  StructField("url", StringType(), True),
  StructField("user", StringType(), True),              # (STRING): User who made the edit or the IP address associated with the anonymous editor
  StructField("userURL", StringType(), True),
  StructField("wikipediaURL", StringType(), True),
  StructField("wikipedia", StringType(), True),         # (STRING): Short name of the Wikipedia that was edited (e.g., "en" for the English)
])

# kafka에서 들어온 데이터 stream 읽고 write해서 bronze급 데이터 만들기 
# 파일형식을 json에서 delta table로 변형하여 저장
from pyspark.sql.functions import from_json, col
(spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "server1.databricks.training:9092")  # Oregon
  #.option("kafka.bootstrap.servers", "server2.databricks.training:9092") # Singapore
  .option("subscribe", "en")
  .load()
  .withColumn("json", from_json(col("value").cast("string"), schema))
  .select(col("timestamp").alias("kafka_timestamp"), col("json.*"))
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath + "/bronze")
  .outputMode("append")
  .start(bronzePath)
)

# stream이 끝난 후 지우고 다시 만들기 
spark.sql("DROP TABLE IF EXISTS WikipediaEditsRaw")

# 아래 Create Table구문은 DB에서 사용하는 Sql 구문
spark.sql("""
  CREATE TABLE WikipediaEditsRaw
  USING Delta
  LOCATION '{}'
""".format(bronzePath))

# 브론즈 데이터 stream형식으로 읽어서 silver데이터 만들기
# delta 테이블에 들어오는 데이터를 select문으로 정의해서 변겨 후 
# 다시 silver 테이블로 append하는 형식
from pyspark.sql.functions import unix_timestamp, col

(spark.readStream
  .format("delta")
  .load(str(bronzePath))
  .select(col("wikipedia"),
          col("isAnonymous"),
          col("namespace"),
          col("page"),
          col("pageURL"),
          col("geocoding"),
          unix_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX").cast("timestamp").alias("timestamp"),
          col("user"))
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath + "/silver")
  .outputMode("append")
  .start(silverPath)
)

spark.sql("DROP TABLE IF EXISTS WikipediaEdits")

spark.sql("""
  CREATE TABLE WikipediaEdits
  USING Delta
  LOCATION '{}'
""".format(silverPath))



from pyspark.sql.functions import col, desc, count

goldDF = (spark.readStream
  .format("delta")
  .load(str(silverPath))
  .withColumn("countryCode", col("geocoding.countryCode3"))
  .filter(col("namespace") == "article")
  .filter(col("countryCode") != "null")
  .filter(col("isAnonymous") == True)
  .groupBy(col("countryCode"))
  .count()
  .withColumnRenamed("count", "total")
  .orderBy(col("total").desc())
)