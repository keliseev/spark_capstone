package capstone.util

import org.apache.avro.LogicalTypes.uuid
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import java.sql.Timestamp
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

case object ClickStreamAggregator
  extends Aggregator[Row, ArrayBuffer[((String, String), (String, String))], Seq[Map[String, String]]] {

  override def zero: ArrayBuffer[((String, String), (String, String))] = ArrayBuffer[((String, String), (String, String))]()

  override def reduce(b: ArrayBuffer[((String, String), (String, String))], a: Row): ArrayBuffer[((String, String), (String, String))] = {

    val (event, time) = (a.getAs[String]("eventType"), a.getAs[Timestamp]("eventTime").toString)
    val attributes = a.getAs[Map[String, String]]("attributes")

    event match {
      case "app_open" =>
        b.append(
          ((event, time), ("campaignId", attributes.getOrElse("campaign_id", null))),
          ((event, time), ("channelId", attributes.getOrElse("channel_id", null))))
      case "purchase" =>
        b.append(((event, time), ("purchaseId", attributes.getOrElse("purchase_id", null))))
      case _ =>
    }

    b
  }

  override def merge(b1: ArrayBuffer[((String, String), (String, String))],
                     b2: ArrayBuffer[((String, String), (String, String))]): ArrayBuffer[((String, String), (String, String))] = {
    b1 ++ b2
  }

  override def finish(reduction: ArrayBuffer[((String, String), (String, String))]): Seq[Map[String, String]] = {

    var uuid = UUID.randomUUID().toString

    val temp = ArrayBuffer[(String, (String, String))]()
    //sort by event time
    val a = reduction.sortBy(_._1._2)

    for (x: ((String, String), (String, String)) <- a) {
      temp.append((uuid, (x._2._1, x._2._2)));
      if (x._1._1 == "purchase") {
        temp.append((uuid, ("sessionId", uuid)))
        uuid = UUID.randomUUID().toString
      }
    }

    temp.groupBy(_._1)
      .values
    //leaving only k -> v pairs from tuples
    .map(_.map(_._2))
    .map(_.toMap)
    .toSeq
  }

  override def bufferEncoder: Encoder[ArrayBuffer[((String, String), (String, String))]] =
    implicitly(ExpressionEncoder[ArrayBuffer[((String, String), (String, String))]])

  override def outputEncoder: Encoder[Seq[Map[String, String]]] =
    implicitly(ExpressionEncoder[Seq[Map[String, String]]])
}

