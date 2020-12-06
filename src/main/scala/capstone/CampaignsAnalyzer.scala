package capstone

import capstone.DemoApp.spark.sqlContext
import capstone.projection.ProjectionWizard.loadProjectionsFromParquet
import org.apache.spark.sql.functions.{col, sum}

object CampaignsAnalyzer {

  def showTopProfitableCampaignsSQL(): Unit = {
    loadProjectionsFromParquet().createOrReplaceTempView("sn")

    sqlContext.sql(
      "SELECT campaignId, sum(billingCost) AS confirmedRevenue " +
        "FROM (SELECT campaignId, billingCost, isConfirmed " +
              "FROM sn WHERE isConfirmed = 'true') " +
        "GROUP BY campaignId ORDER BY confirmedRevenue DESC")
      .show(10, false)
  }

  def showTopProfitableCampaignsAPI(): Unit = {
    loadProjectionsFromParquet()
      .where(col("isConfirmed"))
      .groupBy(col("campaignId"))
      .agg(sum(col("billingCost")).as("confirmedRevenue"))
      .sort(col("confirmedRevenue").desc)
      .show(10, false)
  }
}
