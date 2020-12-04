package capstone

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._

object Demo {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val SESSIONS_PATH = "file:////Users/keliseev/Downloads/GridUCapstone/src/resource/out/sessions.parquet"
  val PURCHASES_PATH = "file:////Users/keliseev/Downloads/GridUCapstone/src/resource/out/purchases.parquet"

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Grid U Demo")
    .master("local")
    .config("spark.ui.port", "4050")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val p_schema: StructType = new StructType()
    .add("purchaseId", StringType)
    .add("purchaseTime", TimestampType)
    .add("billingCost", DecimalType(10, 2))
    .add("isConfirmed", BooleanType)

  //  val session_sch: StructType = new StructType()
  //    .add("userId", StringType)
  //    .add("eventId", StringType)
  //    .add("eventTime", TimestampType)
  //    .add("eventType", StringType)
  //    .add("attributes", MapType(keyType = StringType, valueType = StringType))

  def convertSessionsToParquet(): Unit = spark.read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("src/resource/capstone-dataset/mobile_app_clickstream/mobile_app_clickstream_0.csv.gz")
    .where($"eventType".as[String] === "app_open" || $"eventType".as[String] === "purchase")
    .withColumn("attributes", from_json(col("attributes"), MapType(keyType = StringType, valueType = StringType)))
    .groupBy($"userId")
    .agg(collect_list("attributes").as("attributes"))
    .select($"userId", mergeMaps($"attributes").as("merged_attributes"))
    .select(
      col("userId").as("sessionId"),
      col("merged_attributes").getItem("purchase_id").as("purchaseId"),
      col("merged_attributes").getItem("campaign_id").as("campaignId"),
      col("merged_attributes").getItem("channel_id").as("channelId"))
    .write
    .mode(SaveMode.Overwrite)
    .parquet(SESSIONS_PATH)


  def convertPurchasesToParquet(): Unit = spark.read
    .option("header", "true")
    .schema(p_schema)
    .csv("src/resource/capstone-dataset/user_purchases/user_purchases_0.csv.gz")
    .write
    .mode(SaveMode.Overwrite)
    .parquet(PURCHASES_PATH)

  def loadSessionsFromParquet: DataFrame = spark.read
    .load(SESSIONS_PATH)

  def loadPurchasesFromParquet: DataFrame = spark.read
    .load(PURCHASES_PATH)

  def mostProfitableCampaigns(sessions: DataFrame, purchases: DataFrame): Unit = {
    purchases.where($"isConfirmed")
      .join(sessions, "purchaseId")
      .groupBy($"campaignId")
      .agg(sum($"billingCost").as("confirmedRevenue"))
      .sort($"confirmedRevenue".desc)
      .show(10, false)
  }

  def mostPopularCanalForCampaign(sessions: DataFrame, purchases: DataFrame): Unit = {
    sessions.join(purchases, "purchaseId")
      .groupBy($"campaignId", $"channelId")
      .agg(count($"sessionId").as("uniqueSessions"))
      //
      .groupBy($"campaignId")
      .agg(max($"uniqueSessions").as("maxSessions"))
      //
      .orderBy($"campaignId", $"maxSessions".desc)
      .show(10, false)
  }

  def method1(): Unit = {

    println("Starting...")
    println()

    timed("saving purchases", convertPurchasesToParquet())
    timed("saving sessions", convertSessionsToParquet())

    val p = timed("loading purchases", loadPurchasesFromParquet)
    val s = timed("loading sessions", loadSessionsFromParquet)

    timed("canal for campaign", mostPopularCanalForCampaign(p, s))

  }

  def main(args: Array[String]): Unit = {

    timed("method1", method1())

    println(timing)
    spark.stop()
  }

  def mergeMaps: UserDefinedFunction = udf((data: Seq[Map[String, String]]) => data.reduce(_ ++ _))

  val timing = new StringBuffer

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
