package org.rzlabs.druid

import org.joda.time.{DateTime, DateTimeZone, Interval, Period}
import org.fasterxml.jackson.databind.ObjectMapper._
import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._

import scala.util.Try

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[NoneGranularity], name = "none"),
  new JsonSubTypes.Type(value = classOf[AllGranularity], name = "all"),
  new JsonSubTypes.Type(value = classOf[DurationGranularity], name = "duration"),
  new JsonSubTypes.Type(value = classOf[PeriodGranularity], name = "period")
))
sealed trait DruidQueryGranularity extends Serializable {

  /**
   * The cardinality of the time field amongst the intervals
   * according to the specific granularity.
   * @param ins The intervals specified.
   * @return The cardinality of the time field.
   */
  def ndv(ins: List[Interval]): Long
}

object DruidQueryGranularity {

  def apply(s: String): DruidQueryGranularity = s match {
    case n if n.toLowerCase().equals("none") => NoneGranularity()
    case a if a.toLowerCase().equals("all") => AllGranularity()
    case s if s.toLowerCase().equals("second") => DurationGranularity(1000L)
    case m if m.toLowerCase().equals("minute") => DurationGranularity(60 * 1000L)
    case fm if fm.toLowerCase().equals("fifteen_minute") => DurationGranularity(15 * 60 * 1000L)
    case tm if tm.toLowerCase().equals("thirty_minute") => DurationGranularity(30 * 60 * 1000L)
    case h if h.toLowerCase().equals("hour") => DurationGranularity(3600 * 1000L)
    case d if d.toLowerCase().equals("day") => DurationGranularity(24 * 3600 * 1000L)
    case w if w.toLowerCase().equals("week") => DurationGranularity(7 * 24 * 3600 * 1000L)
    case q if q.toLowerCase().equals("quarter") => DurationGranularity(91 * 24 * 3600 * 1000L)
    case y if y.toLowerCase().equals("year") => DurationGranularity(365 * 24 * 3600 * 1000L)
    case _ => {
//      val jV = parse(s)
//      Try {
//        jV.extract[DurationGranularity]
//      } recover {
//        case _ => jV.extract[PeriodGranularity]
//      } get
      Try {
        jsonMapper.readValue(s, classOf[DurationGranularity])
      } recover {
        case _ => jsonMapper.readValue(s, classOf[PeriodGranularity])
      } get
    }
  }

  def substitute(n: JsonNode): JsonNode = n.findValuesAsText("queryGranularity") match {
    case vl: java.util.List[String] if vl.size > 0 && vl.get(0).nonEmpty =>
      val on = jsonMapper.createObjectNode()
      vl.get(0) match {
        case n if n.toLowerCase().equals("none") => on.put("type", "none")
        case a if a.toLowerCase().equals("all") => on.put("type", "all")
        case s if s.toLowerCase().equals("second") =>
          on.put("type", "duration").put("duration", 1000L)
        case m if m.toLowerCase().equals("minute") =>
          on.put("type", "duration").put("duration", 60 * 1000L)
        case fm if fm.toLowerCase().equals("fifteen_minute") =>
          on.put("type", "duration").put("duration", 15 * 60 * 1000L)
        case tm if tm.toLowerCase().equals("thirty_minute") =>
          on.put("type", "duration").put("duration", 30 * 60 * 1000L)
        case h if h.toLowerCase().equals("hour") =>
          on.put("type", "duration").put("duration", 3600 * 1000L)
        case d if d.toLowerCase().equals("day") =>
          on.put("type", "duration").put("duration", 24 * 3600 * 1000L)
        case w if w.toLowerCase().equals("week") =>
          on.put("type", "duration").put("duration", 7 * 24 * 3600 * 1000L)
        case q if q.toLowerCase().equals("quarter") =>
          on.put("type", "duration").put("duration", 91 * 24 * 3600 * 1000L)
        case y if y.toLowerCase().equals("year") =>
          on.put("type", "duration").put("duration", 365 * 24 * 3600 * 1000L)
        case other => throw new DruidDataSourceException(s"Invalid query granularity '$other'")
      }
      n.asInstanceOf[ObjectNode].replace("queryGranularity", on)
      n
    case _ => n
  }
}

case class AllGranularity() extends DruidQueryGranularity {

  def ndv(ins: List[Interval]) = 1L
}

case class NoneGranularity() extends DruidQueryGranularity {

  def ndv(ins: List[Interval]) = Utils.intervalsMillis(ins)
}

case class DurationGranularity(duration: Long, origin: DateTime = null)
    extends DruidQueryGranularity {

  lazy val originMillis = if (origin == null) 0L else origin.getMillis

  def ndv(ins: List[Interval]) = {
    val boundedIns = ins.flatMap { in =>
      try {
        Some(in.withStartMillis(Math.max(originMillis, in.getStartMillis))
          .withEndMillis(Math.max(originMillis, in.getEndMillis)))
      } catch {
        case e: IllegalArgumentException => None
      }
    }
    Utils.intervalsMillis(boundedIns) / duration
  }
}

case class PeriodGranularity(period: Period,
                             origin: DateTime = null,
                             timeZone: DateTimeZone = null) extends DruidQueryGranularity {

  val tz = if (timeZone == null) DateTimeZone.UTC else timeZone
  lazy val originMillis = if (origin == null) {
    new DateTime(0, DateTimeZone.UTC).withZoneRetainFields(tz).getMillis
  } else {
    origin.getMillis
  }
  lazy val periodMillis = period.getValues.zipWithIndex.foldLeft(0L) {
    case (r, p) => r + p._1 *
      p._2 match {
        case 0 => 365 * 24 * 3600 * 1000L // year
        case 1 => 30 * 24 * 3600 * 1000L  // month
        case 2 => 7 * 24 * 3600 * 1000L   // week
        case 3 => 24 * 3600 * 1000L       // day
        case 4 => 3600 * 1000L            // hour
        case 5 => 60 * 1000L              // minute
        case 6 => 1000L                   // second
        case 7 => 1L                      // millisecond
      }
  }

  def ndv(ins: List[Interval]) = {
    val boundedIns = ins.flatMap { in =>
      try {
        Some(in.withStartMillis(Math.max(originMillis, in.getStartMillis))
          .withEndMillis(Math.max(originMillis, in.getEndMillis)))
      } catch {
        case e: IllegalArgumentException => None
      }
    }
    println("period values === " + period.getValues.foreach(println))
    Utils.intervalsMillis(boundedIns) / periodMillis
  }
}

//class DruidQueryGranularitySerializer extends CustomSerializer[DruidQueryGranularity](format => {
//  implicit val fmt = format
//  (
//    {
//      // PartialFunction used in deserialize method.
//      case jsonObj: JObject =>
//        val fieldMap = jsonObj.values
//        fieldMap.get("type") match {
//          case Some(typ) if typ == "period" =>
//            val period = new Period((jsonObj \ "period").extract[String])
//            //val timeZone: DateTimeZone = Try(DateTimeZone.forID((jsonObj \ "timeZone").extract[String]))
//            //  .recover { case _ => null }
//            val timeZone: DateTimeZone = if (fieldMap.contains("timeZone")) {
//              DateTimeZone.forID((jsonObj \ "timeZone").extract[String])
//            } else null
//            val origin: DateTime = if (fieldMap.contains("origin")) {
//              new DateTime((jsonObj \ "origin").extract[String], timeZone)
//            } else null
//            PeriodGranularity(period, origin, timeZone)
//          case Some(typ) if typ == "duration" =>
//            val duration = (jsonObj \ "duration").extract[Long]
//            val origin: DateTime = if (fieldMap.contains("origin")) {
//              new DateTime((jsonObj \ "origin").extract[String])
//            } else null
//            DurationGranularity(duration, origin)
//        }
//    },
//    {
//      // PartialFunction used in serialize method.
//      case x: DruidQueryGranularity =>
//        throw new RuntimeException("DruidQueryGranularity serialization not supported.")
//    }
//  )
//})