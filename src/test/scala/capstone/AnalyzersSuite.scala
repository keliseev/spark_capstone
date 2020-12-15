package capstone

import capstone.analyzers.{CampaignsAnalyzer, ChannelsAnalyzer}
import capstone.dataloaders.{PurchasesLoader, SessionsLoader}
import capstone.projection.ProjectionWizard
import org.apache.spark.sql.DataFrame
import org.junit._
import org.mockito.Mockito.{mock, when}

class AnalyzersSuite {
  val SessionLoaderMock: SessionsLoader = mock(classOf[SessionsLoader])
  when(SessionLoaderMock.SessionsCSVPath) thenReturn ("src/test/resources/analytics_sessions.csv")
  when(SessionLoaderMock.loadSessionsFromCSV()) thenCallRealMethod()
  val mockedSessions: DataFrame = SessionLoaderMock.loadSessionsFromCSV()

  val PurchasesLoaderMock: PurchasesLoader = mock(classOf[PurchasesLoader])
  when(PurchasesLoaderMock.PurchasesCSVPath) thenReturn ("src/test/resources/analytics_purchases.csv")
  when(PurchasesLoaderMock.loadPurchasesFromCSV()) thenCallRealMethod()
  val mockedPurchases: DataFrame = PurchasesLoaderMock.loadPurchasesFromCSV()

  val ProjectionWizardMock: ProjectionWizard = mock(classOf[ProjectionWizard])
  when(ProjectionWizardMock.sessions) thenReturn (mockedSessions)
  when(ProjectionWizardMock.purchases) thenReturn (mockedPurchases)
  when(ProjectionWizardMock.getProjectionsWithAPI) thenCallRealMethod()
  val mockedProjections: DataFrame = ProjectionWizardMock.getProjectionsWithAPI
  mockedProjections.show(false)

  @Test def `most profitable campaign`(): Unit = {
    val CampaignsAnalyzerMock: CampaignsAnalyzer = mock(classOf[CampaignsAnalyzer])
    when(CampaignsAnalyzerMock.wizard) thenReturn ProjectionWizardMock
    when(CampaignsAnalyzerMock.showTopProfitableCampaignsAPI()) thenCallRealMethod()
    when(CampaignsAnalyzerMock.wizard.loadProjectionsFromParquet()) thenReturn (mockedProjections)

    CampaignsAnalyzerMock.showTopProfitableCampaignsAPI()
  }

  @Test def `most profitable channels`(): Unit = {
    val ChannelsAnalyzerMock: ChannelsAnalyzer = mock(classOf[ChannelsAnalyzer])
    when(ChannelsAnalyzerMock.wizard) thenReturn ProjectionWizardMock
    when(ChannelsAnalyzerMock.showTopChannelsAPI()) thenCallRealMethod()
    when(ChannelsAnalyzerMock.wizard.loadProjectionsFromParquet()) thenReturn (mockedProjections)

    ChannelsAnalyzerMock.showTopChannelsAPI()
  }
}
