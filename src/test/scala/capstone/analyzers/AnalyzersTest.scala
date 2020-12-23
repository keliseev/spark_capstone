package scala.capstone.analyzers

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

import scala.capstone.dao.{ProjectionsDAO, PurchasesDAO, SessionsDAO}

class AnalyzersTest extends AnyFunSuite {

  val dao: ProjectionsDAO = new ProjectionsDAO {
    override val sessions: DataFrame = new SessionsDAO("src/test/resources/analytics_sessions.csv").loadSessionsFromCSV()
    override val purchases: DataFrame = new PurchasesDAO("src/test/resources/analytics_purchases.csv").loadPurchasesFromCSV()
  }

  test("Campaigns analysis test") {
    val campaigns = new CampaignsAnalyzer(dao).getTopCampaignsFromCSV
      .cache()

    assert(campaigns.collect().mkString("Array(", ", ", ")").contains("[c1,101.00]"))
    assert(campaigns.collect().mkString("Array(", ", ", ")").contains("[c2,999.99]"))
  }

  test("Channels analysis test") {
    val channels = new ChannelsAnalyzer(dao).getTopChannelsFromCSV
      .cache()

    assert(channels.collect().mkString("Array(", ", ", ")").contains("[c1,ch1]"))
    assert(channels.collect().mkString("Array(", ", ", ")").contains("[c2,ch3]"))
  }
}
