package capstone

import capstone.analyzers.CampaignsAnalyzer.{showTopProfitableCampaignsAPI, showTopProfitableCampaignsSQL}
import capstone.analyzers.ChannelsAnalyzer.{showTopChannelsAPI, showTopChannelsSQL}
import capstone.dataloaders.PurchasesLoader.{convertPurchasesToParquet, loadPurchasesFromCSV}
import capstone.dataloaders.SessionsLoader.convertSessionsToParquet
import capstone.projection.ProjectionWizard.{getProjectionsWithAPI, refreshProjections}
import org.apache.spark.sql.SparkSession

object DemoApp {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Grid U Demo")
    .master("local")
    .config("spark.ui.port", "4050")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    println("Refreshing data...")

    //building projections
    refreshProjections()

    //getting top 10 marketing campaigns with most confirmed revenue
    showTopProfitableCampaignsSQL()
    showTopProfitableCampaignsAPI()

    //getting channel with most unique sessions for each campaign
    showTopChannelsSQL()
    showTopChannelsAPI()

    spark.stop()
  }
}
