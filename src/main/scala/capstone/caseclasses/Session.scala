package capstone.caseclasses

import java.sql.Timestamp

case class Session(userId: String,
                   eventId: String,
                   eventTime: Timestamp,
                   eventType: String,
                   attributes: Option[Map[String, String]])