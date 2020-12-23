package capstone.analyzers

import capstone.DemoApp.spark.implicits._
import capstone.DemoApp.spark.sqlContext
import capstone.dao.ProjectionsDAO
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}

class ChannelsAnalyzer(projectionsDao: ProjectionsDAO) {

  def showTopChannelsSQL(): Unit = {
    projectionsDao.loadProjectionsFromParquet()
      .createOrReplaceTempView("projections")

    val sqlStatement =
      """
        |SELECT
        | campaignId,
        | channelId
        |FROM (
        | SELECT
        |  campaignId,
        |  channelId,
        |  ROW_NUMBER() OVER (PARTITION BY campaignId
        |               ORDER BY count(sessionId) DESC) AS row
        | FROM
        |  projections
        | GROUP BY
        |  campaignId,
        |  channelId) tmp
        |WHERE
        | row = 1
        |ORDER BY
        | campaignId
      """.stripMargin

    sqlContext.sql(sqlStatement)
      .show(25, truncate = false)
  }

  def showTopChannelsAPI(): Unit = {
    val w = Window.partitionBy($"campaignId")
      .orderBy($"uniqueSessions".desc)

    val temp = projectionsDao
      .loadProjectionsFromParquet()
      .groupBy($"campaignId", $"channelId")
      .agg(count($"sessionId").as("uniqueSessions"))

    temp.withColumn("row", row_number.over(w))
      .where($"row" === 1)
      .drop("row")
      .drop("uniqueSessions")
      .orderBy($"campaignId")
      .show(25, truncate = false)
  }
}
