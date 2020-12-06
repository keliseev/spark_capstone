package capstone.dataloaders

import capstone.DemoApp.spark
import capstone.caseclasses.Session
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SaveMode}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions.{col, collect_list, from_json, udf}
import org.apache.spark.sql.types.{MapType, StringType}

import spark.implicits._

object SessionsLoader {

  def loadSessionsFromCSV(): DataFrame =
    spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/resource/capstone-dataset/mobile_app_clickstream/mobile_app_clickstream_0.csv.gz")
      .withColumn("attributes", from_json(col("attributes"), MapType(keyType = StringType, valueType = StringType)))
      .groupBy(col("userId"))
      .agg(collect_list("attributes").as("attributes"))
      .select(col("userId"), mergeMaps(col("attributes")).as("merged_attributes"))
      .select(
        col("userId").as("sessionId"),
        col("merged_attributes").getItem("purchase_id").as("purchaseId"),
        col("merged_attributes").getItem("campaign_id").as("campaignId"),
        col("merged_attributes").getItem("channel_id").as("channelId"))

  def loadSessionsFromParquet(): DataFrame =
    spark.read
      .load("file:////Users/keliseev/Downloads/GridUCapstone/src/resource/out/sessions.parquet")

  def loadSessionsDataset(): Dataset[Session] = {
    loadSessionsFromParquet().as[Session]
  }

  def convertSessionsToParquet(): Unit =
    loadSessionsFromCSV()
      .write
      .mode(SaveMode.Overwrite)
      .parquet("file:////Users/keliseev/Downloads/GridUCapstone/src/resource/out/sessions.parquet")

  private def mergeMaps: UserDefinedFunction = udf((data: Seq[Map[String, String]]) => data.reduce(_ ++ _))

  private val mapsAggregator = new Aggregator[Map[String, String], Map[String, String], Map[String, String]] {
    def zero: Map[String, String] = Map[String, String]()
    def reduce(b: Map[String, String], a: Map[String, String]): Map[String, String] = a ++ b
    def merge(b1: Map[String, String], b2: Map[String, String]): Map[String, String] = b1 ++ b2
    def finish(b: Map[String, String]): Map[String, String] = b
    override def bufferEncoder: Encoder[Map[String, String]] = implicitly(ExpressionEncoder[Map[String, String]])
    override def outputEncoder: Encoder[Map[String, String]] = implicitly(ExpressionEncoder[Map[String, String]])
  }.toColumn
}
