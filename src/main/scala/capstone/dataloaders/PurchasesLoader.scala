package capstone.dataloaders

import capstone.DemoApp.spark
import capstone.DemoApp.spark.implicits._
import capstone.caseclasses.Purchase
import capstone.util.ConfigLoader
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

object PurchasesLoader extends PurchasesLoader

class PurchasesLoader {
  val PurchasesParquetPath: String = ConfigLoader.purchasesConfig.getString("parquet")
  val PurchasesCSVPath: String = ConfigLoader.purchasesConfig.getString("csv")

  def loadPurchasesFromCSV(): DataFrame =
    spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(PurchasesCSVPath)
      .withColumn("billingCost", $"billingCost".cast(DecimalType(10, 2)))

  def loadPurchasesAsDataset(): Dataset[Purchase] =
    loadPurchasesFromCSV().as[Purchase]

  def convertPurchasesToParquet(): Unit =
    loadPurchasesFromCSV()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(PurchasesParquetPath)
}
