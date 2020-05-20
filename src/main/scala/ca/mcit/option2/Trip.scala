package ca.mcit.option2

import ca.mcit.model.HadoopConnection
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import scala.io.{BufferedSource, Source}

case class Trip(route_id: Int, service_id: String, trip_id: String, trip_headsign: String, direction_id: Int, shape_id: Int,
                wheelchair_accessible: Int, note_fr: String, note_en: String)

object Trip{
  //The file on HDFS
  val filePath = new Path ("/user/fall2019/minhle/project4/trips/trips.txt")
  val stream: FSDataInputStream = HadoopConnection.fs.open(filePath)
  val source: BufferedSource = Source.fromInputStream(stream)

  def extractTrips: Iterator[Trip] = {
    source.getLines().drop(1)
      .map(line => line.split(",",-1))
      .map(a => Trip(a(0).toInt, a(1), a(2), a(3), a(4).toInt, a(5).toInt, a(6).toInt
        , a(7),a(8)))
  }

  def createTripsMap:Map[(String,String), Trip] = {
    extractTrips.map(trip => (trip.trip_id,trip.service_id) -> trip).toMap
  }
}

