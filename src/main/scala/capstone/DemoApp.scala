package capstone

import capstone.analyzers.{CampaignsAnalyzer, ChannelsAnalyzer}
import capstone.dao.ProjectionsDAO
import org.apache.log4j
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

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
  log.setLevel(Level.WARN)

  val projectionsDao: ProjectionsDAO = new ProjectionsDAO()
  val campaignsAnalyzer: CampaignsAnalyzer = new CampaignsAnalyzer(projectionsDao);
  val channelsAnalyzer: ChannelsAnalyzer = new ChannelsAnalyzer(projectionsDao);


  def main(args: Array[String]): Unit = {

    log.info("Refreshing data...")

    projectionsDao.convertProjectionsToParquet()

//    getting top 10 marketing campaigns with most confirmed revenue
    campaignsAnalyzer.showTopProfitableCampaignsAPI()

    //getting channel with most unique sessions for each campaign
    channelsAnalyzer.showTopChannelsAPI()

    spark.stop()
  }
}
