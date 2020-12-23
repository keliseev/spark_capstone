package scala.capstone.analyzers

import org.apache.spark.sql.DataFrame

import capstone.DemoApp.spark.implicits._
import capstone.DemoApp.spark.sqlContext
import capstone.dao.ProjectionsDAO
import org.apache.spark.sql.functions.sum

class CampaignsAnalyzer(projectionsDao: ProjectionsDAO) {

  def getTopProfitableCampaignsSQL(source: DataFrame): DataFrame = {
    source.createOrReplaceTempView("projections")

    val sqlStatement =
      """
        |SELECT
        | campaignId,
        | sum(billingCost) AS confirmedRevenue
        |FROM (
        | SELECT
        |  campaignId,
        |  billingCost,
        |  isConfirmed
        | FROM
        |  projections
        | WHERE
        |  isConfirmed = 'true')
        |GROUP BY
        | campaignId
        |ORDER BY
        | confirmedRevenue DESC
      """.stripMargin

    sqlContext.sql(sqlStatement)
  }

  def showTopProfitableCampaignsSQL(): Unit = {
    getTopProfitableCampaignsSQL(projectionsDao.loadProjectionsFromParquet())
      .show(10, truncate = false)
  }

  def getTopProfitableCampaignsAPI(source: DataFrame): DataFrame = {
    source
      .where($"isConfirmed")
      .groupBy($"campaignId")
      .agg(sum($"billingCost").as("confirmedRevenue"))
      .sort($"confirmedRevenue".desc)
  }

  def showTopProfitableCampaignsAPI(): Unit = {
    getTopProfitableCampaignsAPI(projectionsDao.loadProjectionsFromParquet())
      .show(10, truncate = false)
  }

  def getTopCampaignsFromCSV: DataFrame = getTopProfitableCampaignsAPI(projectionsDao.loadProjectionsAPI)
}
