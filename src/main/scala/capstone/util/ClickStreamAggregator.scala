package capstone.util

import capstone.Model.{EventFlow, SessionParams, SessionWithParams}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Row}

import java.sql.Timestamp
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

case object ClickStreamAggregator extends Aggregator[Row, ArrayBuffer[EventFlow], Seq[SessionParams]] {

  override def zero: ArrayBuffer[EventFlow] = ArrayBuffer[EventFlow]()

  override def reduce(b: ArrayBuffer[EventFlow], a: Row): ArrayBuffer[EventFlow] = {

    val (event, time) = (a.getAs[String]("eventType"), a.getAs[Timestamp]("eventTime").toString)
    val attributes = a.getAs[SessionParams]("attributes")

    event match {
      case "app_open" =>
        b.append(
          ((event, time), ("campaignId", attributes.getOrElse("campaign_id", null))),
          ((event, time), ("channelId", attributes.getOrElse("channel_id", null))))
      case "purchase" =>
        b.append(
          ((event, time), ("purchaseId", attributes.getOrElse("purchase_id", null))))
      case _ =>
    }

    b
  }

  override def merge(b1: ArrayBuffer[EventFlow],
                     b2: ArrayBuffer[EventFlow]): ArrayBuffer[EventFlow] = {
    b1 ++ b2
  }

  override def finish(reduction: ArrayBuffer[EventFlow]): Seq[SessionParams] = {
    var uuid = UUID.randomUUID().toString

    val temp = ArrayBuffer[SessionWithParams]()

    //sorting events by time
    val a = reduction.sortBy(_._1._2)

    //purchase is last event in one session, i.e. we should generate new session id
    for (x: EventFlow <- a) {
      temp.append((uuid, (x._2._1, x._2._2)));
      if (x._1._1 == "purchase") {
        temp.append((uuid, ("sessionId", uuid)))
        uuid = UUID.randomUUID().toString
      }
    }

    //group all events by session id, then mapping them leaving only session attributes
    temp.groupBy(_._1)
      .values
      .map(_.map(_._2))
      .map(_.toMap)
      .toSeq
  }

  override def bufferEncoder: Encoder[ArrayBuffer[EventFlow]] = implicitly(ExpressionEncoder[ArrayBuffer[EventFlow]])

  override def outputEncoder: Encoder[Seq[SessionParams]] = implicitly(ExpressionEncoder[Seq[SessionParams]])
}
