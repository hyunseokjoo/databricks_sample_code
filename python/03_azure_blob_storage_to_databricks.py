"""blobStorage """
storageAccount = ""
containerName = ""
accessKey = ""

spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount), accessKey)


"""Azure Blob 컨테이너의 캐싱 디렉터리를 사용합니다."""
cacheDir = "wasbs://{}@{}.blob.core.windows.net/cacheDir".format(containerName, storageAccount)

customerDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName)
  .load())

customerDF.createOrReplaceTempView("customer_data")

#%sql
#describe customer_data

import uuid
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

uuidUdf = udf(lambda : str(uuid.uuid4()), StringType())
customerUpdatedDF = customerDF.withColumn("CustomerAlternateKey", uuidUdf())
display(customerUpdatedDF)


(customerUpdatedDF.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url", jdbcURI)
  .option("forward_spark_azure_storage_credentials", "true")
  .option("dbtable", tableName + "Staging")
  .option("tempdir", cacheDir)
  .save())

customerTempDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName + "Staging")
  .load())

customerDF.createOrReplaceTempView("customer_temp_data")