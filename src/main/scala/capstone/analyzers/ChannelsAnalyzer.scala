package capstone.analyzers

import capstone.DemoApp.spark.implicits._
import capstone.DemoApp.spark.sqlContext
import capstone.projection.ProjectionWizard
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}

object ChannelsAnalyzer extends ChannelsAnalyzer

class ChannelsAnalyzer {

  val wizard: ProjectionWizard = ProjectionWizard

  def showTopChannelsSQL(): Unit = {
    wizard.loadProjectionsFromParquet()
      .createOrReplaceTempView("projections")

    val sqlStatement =
      """
       SELECT campaignId,
              channelId
       FROM
         (SELECT campaignId,
                 channelId,
                 RANK() OVER (PARTITION BY campaignId
                              ORDER BY count(sessionId) DESC) AS rank
          FROM projections
          GROUP BY campaignId,
                   channelId) tmp
       WHERE rank = 1
       ORDER BY campaignId
      """.stripMargin

    sqlContext.sql(sqlStatement)
      .show(25, false)
  }

  def showTopChannelsAPI(): Unit = {
    val w = Window.partitionBy($"campaignId")
      .orderBy($"uniqueSessions".desc)

    val temp = wizard
      .loadProjectionsFromParquet()
      .groupBy($"campaignId", $"channelId")
      .agg(count($"sessionId").as("uniqueSessions"))

    temp.withColumn("row", row_number.over(w))
      .where($"row" === 1)
      .drop("row")
      .drop("uniqueSessions")
      .orderBy($"campaignId")
      .show(25, false)
  }
}
