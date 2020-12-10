package capstone

import capstone.analyzers.CampaignsAnalyzer.{showTopProfitableCampaignsAPI, showTopProfitableCampaignsSQL}
import capstone.analyzers.ChannelsAnalyzer.{showTopChannelsAPI, showTopChannelsSQL}
import capstone.dataloaders.PurchasesLoader.convertPurchasesToParquet
import capstone.dataloaders.SessionsLoader.convertSessionsToParquet
import capstone.projection.ProjectionWizard.refreshProjections
import capstone.util.TimingUtil.timed
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

    //load data from CSV
    timed("Loading sessions from csv", convertSessionsToParquet())
    timed("Loading purchases from csv", convertPurchasesToParquet())
    //building projections
    timed("Creating projections for sessions with purchases", refreshProjections())

    //getting top 10 marketing campaigns with most confirmed revenue
    timed("Top 10 campaigns with SQL", showTopProfitableCampaignsSQL())
    timed("Top 10 campaigns with API", showTopProfitableCampaignsAPI())

    //getting channel with most unique sessions for each campaign
    timed("Most popular channel per campaign SQL", showTopChannelsSQL())
    timed("Most popular channel per campaign API", showTopChannelsAPI())

    spark.stop()
  }
}
