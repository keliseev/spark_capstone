package capstone.projection

import capstone.DemoApp.spark
import capstone.DemoApp.spark.sqlContext
import capstone.caseclasses.Projection
import capstone.dataloaders.PurchasesLoader.loadPurchasesFromParquet
import capstone.dataloaders.SessionsLoader.loadSessionsFromParquet
import capstone.util.ConfigLoader
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

object ProjectionWizard extends ProjectionWizard

class ProjectionWizard {
  val ProjectionsParquetPath: String = ConfigLoader.projectionConfig.getString("parquet")

  def sessions: DataFrame = loadSessionsFromParquet()

  def purchases: DataFrame = loadPurchasesFromParquet()

  def getProjectionsWithSQL: DataFrame = {
    sessions.createOrReplaceTempView("sessions")
    purchases.createOrReplaceTempView("purchases")

    val sqlStatement =
      """
       SELECT
        purchases.purchaseId,
        purchaseTime,
        billingCost,
        isConfirmed,
        sessionId,
        campaignId,
        channelId
       FROM sessions
       JOIN purchases ON sessions.purchaseId = purchases.purchaseId
      """.stripMargin

    sqlContext.sql(sqlStatement)
  }

  def getProjectionsWithAPI: DataFrame = sessions.join(purchases, "purchaseId")

  def loadProjectionsFromParquet(): DataFrame = {
    spark.read
      .load(ProjectionsParquetPath)
  }

  import spark.implicits._

  def loadProjectionsDataset(): Dataset[Projection] =
    loadProjectionsFromParquet().as[Projection]

  def refreshProjections(): Unit =
    getProjectionsWithAPI
      .write
      .mode(SaveMode.Overwrite)
      .parquet(ProjectionsParquetPath)
}
