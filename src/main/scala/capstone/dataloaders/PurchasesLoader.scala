package capstone.dataloaders

import capstone.DemoApp.spark
import capstone.caseclasses.Purchase
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

object PurchasesLoader {

  def loadPurchasesFromCSV(): DataFrame =
    spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/resource/capstone-dataset/user_purchases/user_purchases_0.csv.gz")

  def loadPurchasesFromParquet(): DataFrame =
    spark.read
      .load("file:////Users/keliseev/Downloads/GridUCapstone/src/resource/out/purchases.parquet")

  import spark.implicits._

  def loadPurchasesDataset(): Dataset[Purchase] = {
    loadPurchasesFromParquet().as[Purchase]
  }

  def convertPurchasesToParquet(): Unit =
    loadPurchasesFromCSV()
      .write
      .mode(SaveMode.Overwrite)
      .parquet("file:////Users/keliseev/Downloads/GridUCapstone/src/resource/out/purchases.parquet")
}
