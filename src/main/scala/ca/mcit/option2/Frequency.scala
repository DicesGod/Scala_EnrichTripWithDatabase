package ca.mcit.option2

import ca.mcit.model.HadoopConnection
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import scala.io.{BufferedSource, Source}

case class Frequency(trip_id: String, start_time: String, end_time: String, headway_secs: Int)

object Frequency{
  //The file on HDFS
  val filePath = new Path ("/user/fall2019/minhle/project4/frequencies/frequencies.txt")
  val stream: FSDataInputStream = HadoopConnection.fs.open(filePath)
  val source: BufferedSource = Source.fromInputStream(stream)

  def extractFrequencies: Iterator[Frequency] = {
    source.getLines().drop(1)
      .map(line => line.split(",",-1))
      .map(a => Frequency(a(0), a(1), a(2), a(3).toInt))
  }

  def createFrequenciesMap:Map[String, Frequency] = {
    extractFrequencies.map(frequencies => frequencies.trip_id -> frequencies).toMap
  }
}

