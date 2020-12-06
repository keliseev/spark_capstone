package capstone

import capstone.projection.ProjectionWizard.loadProjectionsFromParquet
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, row_number, max}

object ChannelsAnalyzer {

  def showTopChannelsSQL(): Unit = {
    loadProjectionsFromParquet().createOrReplaceTempView("sn")

  }

  def showTopChannelsAPI(): Unit = {
    val w = Window.partitionBy(col("campaignId"))
      .orderBy(col("uniqueSessions").desc)

    val temp = loadProjectionsFromParquet()
      .groupBy(col("campaignId"), col("channelId"))
      .agg(count(col("sessionId")).as("uniqueSessions"))

    temp.withColumn("row",row_number.over(w))
      .where(col("row") === 1)
      .drop("row")
      .orderBy(col("campaignId"))
      .show(25, false)
  }
}
