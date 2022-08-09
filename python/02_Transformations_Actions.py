"""The data is located at dbfs:/mnt/training/initech/products/dirty.csv. 으로 가정"""

# %fs ls /mnt/training/initech/ 저장소 연결

# 데이터 불러오기
CSV_FILE = "dbfs:/mnt/training/initech/products/dirty.csv"

retailDF = (spark.read                     # Our DataFrameReader
  .option("header", True)
  .option("inferSchema", True)
  .csv(CSV_FILE)
)

retailDF.printSchema()
retailDF.show()

"""display(..) 함수를 사용하여 더 자세한 내용 확인 가능"""
retailDF.display()

"""limit"""
limitedDF = retailDF.limit(5)
limitedDF.show(100, False)
total = retailDF.count()

"""select"""
selectDF = retailDF.select("product_id", "category", "brand", "model", "price")
selectDF.printSchema()

"""drop"""
droppedDF = retailDF.drop("processor", "size", "display")
droppedDF.printSchema()

"""withColumnRenamed(..)"""
droppedDF.withColumnRenamed("product_id", "prodID").printSchema()

"""withColumn(..)"""
from pyspark.sql.functions import col
doublePriceDF = droppedDF.withColumn("doublePrice", col("price") * 2)
doublePriceDF.show(3)

droppedDF.withColumn("doublePrice", droppedDF["price"] * 2).show(3)


"""selectExpr(..)"""
display(retailDF.selectExpr("product_id as prodID", "category", "brand","price", "price*2 as 2xPrice"))

"""filter(..)"""
display(retailDF.filter(col("price") >= 2000))
display(retailDF.filter("price >= 2000"))

"""groupBy(..)"""
display(retailDF.groupBy("brand")
  .count()
  .orderBy(col("count").desc())
)

"""take(n)"""
items = retailDF.take(10)
for i in items:
  print(i)

"""collect(..)"""
items = (retailDF.groupBy("brand")
  .count()
  .limit(40)
  .collect()
)

for i in items:
  print(i)