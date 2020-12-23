package scala.capstone.analyzers

import org.apache.spark.sql.DataFrame

import capstone.DemoApp.spark.implicits._
import capstone.DemoApp.spark.sqlContext
import capstone.dao.ProjectionsDAO
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}

class ChannelsAnalyzer(projectionsDao: ProjectionsDAO) {

  def getTopChannelsSQL(source: DataFrame): DataFrame = {
    source.createOrReplaceTempView("projections")

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
  }

  def showTopChannelsSQL(): Unit =
    getTopChannelsSQL(projectionsDao.loadProjectionsFromParquet())
      .show(25, truncate = false)

  def getTopChannelsAPI(source: DataFrame): DataFrame = {
    val w = Window.partitionBy($"campaignId")
      .orderBy($"uniqueSessions".desc)

    val temp =
      source
      .groupBy($"campaignId", $"channelId")
      .agg(count($"sessionId").as("uniqueSessions"))

    temp.withColumn("row", row_number.over(w))
      .where($"row" === 1)
      .drop("row")
      .drop("uniqueSessions")
      .orderBy($"campaignId")
  }

  def showTopChannelsAPI(): Unit = getTopChannelsAPI(projectionsDao.loadProjectionsFromParquet()).show(25, truncate = false)

  def getTopChannelsFromCSV: DataFrame = getTopChannelsAPI(projectionsDao.loadProjectionsAPI)
}
