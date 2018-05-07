package org.rzlabs.druid

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.MyLogging
import org.joda.time.Interval
import org.fasterxml.jackson.databind.ObjectMapper._

object Utils extends MyLogging {

//  implicit val jsonFormat = Serialization.formats(
//    ShortTypeHints(
//      List(
//        classOf[DruidRelationColumnInfo],
//        classOf[DurationGranularity],
//        classOf[PeriodGranularity]
//      )
//    )
//  ) + new DruidQueryGranularitySerializer

  def intervalsMillis(intervals: List[Interval]): Long = {
    intervals.foldLeft[Long](0L) {
      case (t, in) => t + (in.getEndMillis - in.getStartMillis)
    }
  }

  def updateInterval(interval: Interval, `with`: Interval, withType: String) = {
    interval
      .withStartMillis(Math.min(interval.getStartMillis, `with`.getStartMillis))
      .withEndMillis(Math.max(interval.getEndMillis, `with`.getEndMillis))
  }

  def filterSomes[A](a: List[Option[A]]): List[Option[A]] = {
    a.filter { case Some(x) => true; case None => false }
  }

  /**
   * transform List[Option] tp Option[List]
   * @param a
   * @tparam A
   * @return
   */
  def sequence[A](a: List[Option[A]]): Option[List[A]] = a match {
    case Nil => Some(Nil)
    case head :: tail => head.flatMap (h => sequence(tail).map(h :: _))
  }

  def toPrettyJson(obj: Either[AnyRef, JsonNode]) = {
    jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(
      if (obj.isLeft) obj.left else obj.right)
  }


}
