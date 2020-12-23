package capstone.analyzers

import capstone.DemoApp.spark.implicits._
import capstone.DemoApp.spark.sqlContext
import capstone.dao.ProjectionsDAO
import org.apache.spark.sql.functions.sum

class CampaignsAnalyzer(projectionsDao: ProjectionsDAO) {

  def showTopProfitableCampaignsSQL(): Unit = {
    projectionsDao.loadProjectionsFromParquet()
      .createOrReplaceTempView("projections")

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
      .show(10, truncate = false)
  }

  def showTopProfitableCampaignsAPI(): Unit = {
    projectionsDao.loadProjectionsFromParquet()
      .where($"isConfirmed")
      .groupBy($"campaignId")
      .agg(sum($"billingCost").as("confirmedRevenue"))
      .sort($"confirmedRevenue".desc)
      .show(10, truncate = false)
  }
}
