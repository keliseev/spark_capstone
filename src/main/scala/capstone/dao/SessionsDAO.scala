package capstone.dao

import capstone.DemoApp.spark
import capstone.DemoApp.spark.implicits._
import capstone.model.Session
import capstone.util.{ClickStreamAggregator, ConfigLoader}
import org.apache.spark.sql.functions.{explode, from_json}
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

class SessionsDAO(csvPath: String) {

  val parquetPath: String = ConfigLoader.sessionsConfig.getString("parquet")

  def loadSessionsFromCSV(): DataFrame = {
    spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(csvPath)
      .withColumn("attributes", from_json($"attributes", MapType(keyType = StringType, valueType = StringType)))
      .orderBy($"userId", $"eventTime")
      .groupBy($"userId")
      .agg(ClickStreamAggregator.toColumn.alias("sessionParams"))
      .select($"userId", explode($"sessionParams"))
      .select(
        $"col".getItem("sessionId").as("sessionId"),
        $"col".getItem("purchaseId").as("purchaseId"),
        $"col".getItem("campaignId").as("campaignId"),
        $"col".getItem("channelId").as("channelId"))
  }

  def loadSessionsFromCSVWithSQL: DataFrame =
    spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(csvPath)

  def loadSessionsAsDataset(): Dataset[Session] =
    loadSessionsFromCSV().as[Session]

  def saveCSVSessionsToParquet(): Unit =
    loadSessionsFromCSV()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(parquetPath)
}