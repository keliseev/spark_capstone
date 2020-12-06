package capstone

import capstone.CampaignsAnalyzer.{showTopProfitableCampaignsAPI, showTopProfitableCampaignsSQL}
import capstone.ChannelsAnalyzer.{showTopChannelsAPI, showTopChannelsSQL}
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
    println("Starting...")
    println()

    //        timed("campaigns api", showTopProfitableCampaignsAPI())
    //        timed("campaigns sql", showTopProfitableCampaignsSQL())

    timed("campaigns sql", showTopChannelsSQL())
    timed("campaigns api", showTopChannelsAPI())

    spark.stop()
  }
}
