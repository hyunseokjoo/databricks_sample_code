
dataPath = "/mnt/training/definitive-guide/data/activity-data"
outputPath = userhome + "/gaming"
basePath = userhome + "/advanced-streaming"
checkpointPath = basePath + "/checkpoints"
activityPath = basePath + "/activityCount"


static = spark.read.json(dataPath)
dataSchema = static.schema

"""
writeStream과 같은 작업이 호출되면 스트리밍 데이터의 각 행이 DataFrame의 행이 됩니다.
참고 사항 이 강의에서는 사용자가 가질 수 있는 파일 할당량을 초과하지 않도록 옵션("maxFilesPerTrigger", 1)을 사용하여 트리거당 하나의 파일로 스트림 흐름을 제한합니다. 기본값은 1000입니다.
Arrival_Time을 타임스탬프 형식으로 변환합니다. 인덱스 이름을 User_ID로 바꿉니다.
"""
deltaStreamWithTimestampDF = (spark
  .readStream
  .format("delta")
  .option("maxFilesPerTrigger", 1)
  .schema(dataSchema)
  .json(dataPath)
  .withColumnRenamed('Index', 'User_ID')
  .selectExpr("*","cast(cast(Arrival_Time as double)/1000 as timestamp) as event_time")
)

# output 지정
deltaStreamingQuery = (deltaStreamWithTimestampDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath)
  .outputMode("append")
  .start(basePath)
)

# 확인
for s in spark.streams.active:
  print(s.id)

# 다시 basePath에서 load 하여 stream으로 만들고 다시 write하기
# 집계 작업을 수행하려면 어떤 종류의 outputMode를 complete를 사용하면 된다.
activityCountsQuery = (spark.readStream
  .format("delta")
  .load(str(basePath))
  .groupBy("gt")
  .count()
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath + "/activityCount")
  .outputMode("complete")
  .start(activityPath)
)

# windowing
from pyspark.sql.functions import hour, window, col

countsDF = (deltaStreamWithTimestampDF
  .withWatermark("event_time", "180 minutes")
  .groupBy(window("event_time", "60 minute"),"gt")
  .count()
  .withColumn('hour',hour(col('window.start')))
)

display(countsDF.withColumn('hour',hour(col('window.start'))))

# Stop streams.
for streamingQuery in spark.streams.active:
  streamingQuery.stop()