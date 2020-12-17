package capstone.analyzers

import capstone.DemoApp.spark.implicits._
import capstone.DemoApp.spark.sqlContext
import capstone.projection.ProjectionWizard
import org.apache.spark.sql.functions.sum

object CampaignsAnalyzer extends CampaignsAnalyzer

class CampaignsAnalyzer {

  val wizard: ProjectionWizard = ProjectionWizard

  def showTopProfitableCampaignsSQL(): Unit = {
    wizard.loadProjectionsFromParquet()
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
      .show(10, false)
  }

  def showTopProfitableCampaignsAPI(): Unit = {
    wizard.loadProjectionsFromParquet()
      .where($"isConfirmed")
      .groupBy($"campaignId")
      .agg(sum($"billingCost").as("confirmedRevenue"))
      .sort($"confirmedRevenue".desc)
      .show(10, false)
  }
}
