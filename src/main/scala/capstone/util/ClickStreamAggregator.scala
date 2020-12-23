package capstone.util

import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

case object ClickStreamAggregator
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
          (uuid, ("channelId", attributes.getOrElse("channel_id", null))))
      case "purchase" =>
        b.append((uuid, ("purchaseId", attributes.getOrElse("purchase_id", null))))
      case _ =>
    }

    b
  }

  override def merge(b1: ArrayBuffer[(String, (String, String))],
                     b2: ArrayBuffer[(String, (String, String))]): ArrayBuffer[(String, (String, String))] = {
    b1 ++ b2
  }

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

