package capstone.model

import org.apache.spark.sql.types.Decimal
import java.sql.Timestamp

case class Purchase(purchaseId: String,
                    purchaseTime: Timestamp,
                    billingCost: Double,
                    isConfirmed: Boolean)

case class Session(userId: String,
                   eventId: String,
                   eventTime: Timestamp,
                   eventType: String,
                   attributes: Option[Map[String, String]])

case class Projection(purchaseId: String,
                      purchaseTime: Timestamp,
                      billingCost: Decimal,
                      isConfirmed: Boolean,
                      sessionId: String,
                      campaignId: String,
                      channelId: String)