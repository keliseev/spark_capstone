package capstone.caseclasses

import org.apache.spark.sql.types.Decimal

import java.sql.Timestamp

case class Projection(purchaseId: String,
                      purchaseTime: Timestamp,
                      billingCost: Decimal,
                      isConfirmed: Boolean,
                      sessionId: String,
                      campaignId: String,
                      channelId: String)
