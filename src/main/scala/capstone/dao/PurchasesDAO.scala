package capstone.dao

import capstone.DemoApp.spark
import capstone.DemoApp.spark.implicits._
import capstone.Model.Purchase
import capstone.util.ConfigLoader
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

class PurchasesDAO(csvPath: String) {

  val parquetPath: String = ConfigLoader.purchasesConfig.getString("parquet")

  def loadPurchasesFromCSV(): DataFrame =
    spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(csvPath)
      .withColumn("billingCost", $"billingCost".cast(DecimalType(10, 2)))

  def loadPurchasesAsDataset(): Dataset[Purchase] =
    loadPurchasesFromCSV().as[Purchase]

  def saveCSVPurchasesToParquet(): Unit =
    loadPurchasesFromCSV()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(parquetPath)
}
