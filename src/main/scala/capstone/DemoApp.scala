package capstone

import capstone.analyzers.{CampaignsAnalyzer, ChannelsAnalyzer}
import capstone.dao.ProjectionsDAO
import org.apache.log4j
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.lang.System.Logger

object DemoApp {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Grid U Demo")
    .master("local[*]")
    .config("spark.ui.port", "4050")
    .config("spark.default.parallelism", "1")
    .config("spark.sql.shuffle.partitions", "10")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val log: log4j.Logger = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  val projectionsDao: ProjectionsDAO = new ProjectionsDAO()
  val campaignsAnalyzer: CampaignsAnalyzer = new CampaignsAnalyzer(projectionsDao);
  val channelsAnalyzer: ChannelsAnalyzer = new ChannelsAnalyzer(projectionsDao);


  def main(args: Array[String]): Unit = {


    log.info("Refreshing data...")

    projectionsDao.sessions.show(50, false)
//    projectionsDao.purchases.sort("billingCost").show(150)
//    projectionsDao.convertProjectionsToParquet()
//    projectionsDao.loadProjectionsFromParquet().sort("campaignId").show(150, false)
    //building projections
//    projectionsDao.sessions.show(200)

//    projectionsDao.loadProjectionsFromParquet().show(150, false)

    //getting top 10 marketing campaigns with most confirmed revenue
//    campaignsAnalyzer.showTopProfitableCampaignsAPI()

    //getting channel with most unique sessions for each campaign
//    channelsAnalyzer.showTopChannelsAPI()

    spark.stop()
  }
}
