package scala.capstone.dao

import org.scalatest.funsuite.AnyFunSuite

class SessionsDAOTest extends AnyFunSuite {

  test("User with two sessions test") {
    val dao: SessionsDAO = new SessionsDAO("src/test/resources/user_with_two_sessions.csv")
    val sessionsNumber = dao.loadSessionsFromCSV().count()
    assert(sessionsNumber === 2)
  }
}
