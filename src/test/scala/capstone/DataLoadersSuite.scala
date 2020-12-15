package capstone

import capstone.DemoApp.spark.implicits._
import capstone.caseclasses.Projection
import capstone.dataloaders.{PurchasesLoader, SessionsLoader}
import capstone.projection.ProjectionWizard
import org.apache.spark.sql.DataFrame
import org.junit._
import org.mockito.Mockito.{mock, when}

class DataLoadersSuite {
  val SessionLoaderMock: SessionsLoader = mock(classOf[SessionsLoader])
  when(SessionLoaderMock.SessionsCSVPath) thenReturn ("src/test/resources/user_with_two_sessions.csv")
  when(SessionLoaderMock.loadSessionsFromCSV()) thenCallRealMethod()
  val mockedSessions: DataFrame = SessionLoaderMock.loadSessionsFromCSV()

  @Test def `loadSessionsFromCSV separates sessions of one user`(): Unit = {
    assert(mockedSessions.count() != 3L)
    assert(mockedSessions.count() == 2L)
  }

  @Test def `merge of sessions and purchases goes correctly`(): Unit = {
    val PurchasesLoaderMock: PurchasesLoader = mock(classOf[PurchasesLoader])
    when(PurchasesLoaderMock.PurchasesCSVPath) thenReturn ("src/test/resources/purchase_for_two_sessions_user.csv")
    when(PurchasesLoaderMock.loadPurchasesFromCSV()) thenCallRealMethod()
    val mockedPurchases: DataFrame = PurchasesLoaderMock.loadPurchasesFromCSV()

    val ProjectionWizardMock: ProjectionWizard = mock(classOf[ProjectionWizard])
    when(ProjectionWizardMock.sessions) thenReturn (mockedSessions)
    when(ProjectionWizardMock.purchases) thenReturn (mockedPurchases)
    when(ProjectionWizardMock.getProjectionsWithAPI) thenCallRealMethod
    val projections = ProjectionWizardMock.getProjectionsWithAPI.as[Projection]

    assert(projections.rdd.filter(_.campaignId == "123").first().isConfirmed)
    assert(projections.rdd.filter(_.campaignId == "764").first().channelId == "VK Ads")
  }
}
