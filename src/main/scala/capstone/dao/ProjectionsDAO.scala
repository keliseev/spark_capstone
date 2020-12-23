package scala.capstone.dao

import capstone.DemoApp.spark
import capstone.DemoApp.spark.implicits._
import capstone.DemoApp.spark.sqlContext
import capstone.Model.Projection
import capstone.util.ConfigLoader
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

class ProjectionsDAO {

  val ProjectionsParquetPath: String = ConfigLoader.projectionConfig.getString("parquet")

  val sessions: DataFrame = new SessionsDAO(ConfigLoader.sessionsConfig.getString("csv")).loadSessionsFromCSV()
  val purchases: DataFrame = new PurchasesDAO(ConfigLoader.purchasesConfig.getString("csv")).loadPurchasesFromCSV()

  def loadProjectionsSQL: DataFrame = {
    sessions.createOrReplaceTempView("sessions")
    purchases.createOrReplaceTempView("purchases")

    val sqlStatement =
      """
        |SELECT purchases.purchaseId,
        |       purchaseTime,
        |       billingCost,
        |       isConfirmed,
        |       sessionId,
        |       campaignId,
        |       channelId
        |FROM sessions
        |JOIN purchases ON sessions.purchaseId = purchases.purchaseId
      """.stripMargin

    sqlContext.sql(sqlStatement)
  }

  def loadProjectionsAPI: DataFrame = sessions.join(purchases, "purchaseId")

  def loadProjectionsFromParquet(): DataFrame = {
    spark.read
      .load(ProjectionsParquetPath)
  }

  def loadProjectionsAsDataset(): Dataset[Projection] = {
    loadProjectionsFromParquet().as[Projection]
  }

  def refreshProjections(): Unit = {
    loadProjectionsAPI
      .write
      .mode(SaveMode.Overwrite)
      .parquet(ProjectionsParquetPath)
  }
}
