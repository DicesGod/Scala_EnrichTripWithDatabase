package ca.mcit.option2

import ca.mcit.model.HadoopConnection
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import scala.io.{BufferedSource, Source}

case class Calendar_date(service_id: String, date: String, exception_type: Int)

object Calendar_date{
  //The file on HDFS
  val filePath = new Path ("/user/fall2019/minhle/project4/calendar_dates/calendar_dates.txt")
  val stream: FSDataInputStream = HadoopConnection.fs.open(filePath)
  val source: BufferedSource = Source.fromInputStream(stream)

  def extractCalendar_dates: Iterator[Calendar_date] = {
    source.getLines().drop(1)
      .map(line => line.split(",",-1))
      .map(a => Calendar_date(a(0), a(1), a(2).toInt))
  }

  def createCalendar_datesMap:Map[String, Calendar_date] = {
    extractCalendar_dates.map(calendar_dates => calendar_dates.service_id -> calendar_dates).toMap
  }
}

