package capstone.projection

import capstone.DemoApp.spark
import capstone.DemoApp.spark.sqlContext
import capstone.caseclasses.Projection
import capstone.dataloaders.PurchasesLoader.loadPurchasesFromParquet
import capstone.dataloaders.SessionsLoader.loadSessionsFromParquet
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

object ProjectionWizard {

  def getProjectionsWithSQL: DataFrame = {
    loadSessionsFromParquet().createOrReplaceTempView("s")
    loadPurchasesFromParquet().createOrReplaceTempView("p") //TODO Change name of table to some sensible

//TODO Move all sql statements to seprate variables, apply SQL format and add .stripMargin function
    val sqlStatement =
      """
       SELECT
        p.purchaseId,
        purchaseTime,
        billingCost,
        isConfirmed,
        sessionId,
        campaignId,
        channelId
       FROM s
       JOIN p ON s.purchaseId = p.purchaseId
      """.stripMargin

    sqlContext.sql(
      "SELECT p.purchaseId, purchaseTime, billingCost, " +
                     "isConfirmed, sessionId, campaignId, channelId " +
        "FROM s JOIN p ON s.purchaseId = p.purchaseId")
  }

  def getProjectionsWithAPI: DataFrame = {
    loadSessionsFromParquet().join(loadPurchasesFromParquet(), "purchaseId")
  }

  def loadProjectionsFromParquet(): DataFrame = {
    spark.read
      .load("file:////Users/keliseev/Downloads/GridUCapstone/src/resource/out/projections.parquet") //TODO please use relative path.
  }

  import spark.implicits._

  def loadProjectionsDataset(): Dataset[Projection] = {
    loadProjectionsFromParquet().as[Projection]
  }

  def convertProjectionsToParquet(): Unit = {
    getProjectionsWithAPI
      .write
      .mode(SaveMode.Overwrite)
      .parquet("file:////Users/keliseev/Downloads/GridUCapstone/src/resource/out/projections.parquet") //TODO please use relative path.
  }
}
