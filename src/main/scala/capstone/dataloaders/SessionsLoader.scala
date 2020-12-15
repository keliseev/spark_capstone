package capstone.dataloaders

import capstone.DemoApp.spark
import capstone.DemoApp.spark.implicits._
import capstone.caseclasses.Session
import capstone.dataloaders.SessionsLoader.clickStreamAggregator
import capstone.util.ConfigLoader
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.{explode, from_json}
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql._

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

object SessionsLoader extends SessionsLoader {

  case object clickStreamAggregator
    extends Aggregator[Row, ArrayBuffer[(String, (String, String))], Seq[Map[String, String]]] {

    private var uuid = ""

    override def zero: ArrayBuffer[(String, (String, String))] = ArrayBuffer[(String, (String, String))]()

    override def reduce(b: ArrayBuffer[(String, (String, String))], a: Row): ArrayBuffer[(String, (String, String))] = {
      val event = a.getAs[String]("eventType")
      val attributes = a.getAs[Map[String, String]]("attributes")

      event match {
        case "app_open" =>
          uuid = UUID.randomUUID().toString
          b.append(
            (uuid, ("sessionId", uuid)),
            (uuid, ("campaignId", attributes.getOrElse("campaign_id", null))),
            (uuid, ("channelId", attributes.getOrElse("channel_id", null)))
          )
        case "purchase" =>
          b.append((uuid, ("purchaseId", attributes.getOrElse("purchase_id", null))))
        case _ =>
      }

      b
    }

    override def merge(b1: ArrayBuffer[(String, (String, String))],
                       b2: ArrayBuffer[(String, (String, String))]): ArrayBuffer[(String, (String, String))] = b1 ++ b2

    override def finish(reduction: ArrayBuffer[(String, (String, String))]): Seq[Map[String, String]] = {
      reduction
        //grouping by session id
        .groupBy(_._1)
        .values
        //leaving only k -> v pairs from tuples
        .map(_.map(_._2))
        .map(_.toMap)
        .toSeq
    }

    override def bufferEncoder: Encoder[ArrayBuffer[(String, (String, String))]] =
      implicitly(ExpressionEncoder[ArrayBuffer[(String, (String, String))]])

    override def outputEncoder: Encoder[Seq[Map[String, String]]] =
      implicitly(ExpressionEncoder[Seq[Map[String, String]]])
  }
}

class SessionsLoader {
  val SessionsParquetPath: String = ConfigLoader.sessionsConfig.getString("parquet")
  val SessionsCSVPath: String = ConfigLoader.sessionsConfig.getString("csv")

  def loadSessionsFromCSV(): DataFrame =
    spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(SessionsCSVPath)
      .withColumn("attributes", from_json($"attributes", MapType(keyType = StringType, valueType = StringType)))
      .orderBy($"userId", $"eventTime")
      .groupBy($"userId")
      .agg(clickStreamAggregator.toColumn.alias("sessionParams"))
      .select($"userId", explode($"sessionParams"))
      .select(
        $"col".getItem("sessionId").as("sessionId"),
        $"col".getItem("purchaseId").as("purchaseId"),
        $"col".getItem("campaignId").as("campaignId"),
        $"col".getItem("channelId").as("channelId"))

  def loadSessionsFromParquet(): DataFrame =
    spark.read
      .load(SessionsParquetPath)

  def loadSessionsDataset(): Dataset[Session] =
    loadSessionsFromParquet().as[Session]

  def convertSessionsToParquet(): Unit =
    loadSessionsFromCSV()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(SessionsParquetPath)
}
