package ca.mcit.option2

import java.io.{BufferedWriter, OutputStreamWriter}
import au.com.bytecode.opencsv.CSVWriter
import ca.mcit.model.HadoopConnection
import org.apache.hadoop.fs.{FSDataOutputStream, Path}

class FileManagement {
  def createPartitionFolder: Any = {
    var filePath = new Path("/user/hive/warehouse/fall2019_minhle.db/tem_enriched_trip/wheelchair_accessible=1")
    if (HadoopConnection.fs.exists(filePath)) {
      HadoopConnection.fs.delete(filePath, true)
      println(" tem_enriched_trip/wheelchair_accessible=1 directory exists")
      HadoopConnection.fs.mkdirs(filePath)
      println(" tem_enriched_trip/wheelchair_accessible=1 is deleted and recreated")
    }
    else {
      HadoopConnection.fs.mkdirs(filePath)
      println(" tem_enriched_trip/wheelchair_accessible=1 is recreated")
    }

    filePath = new Path("/user/hive/warehouse/fall2019_minhle.db/tem_enriched_trip/wheelchair_accessible=2")
    if (HadoopConnection.fs.exists(filePath)) {
      HadoopConnection.fs.delete(filePath, true)
      println(" tem_enriched_trip/wheelchair_accessible=2 directory exists")
      HadoopConnection.fs.mkdirs(filePath)
      println(" tem_enriched_trip/wheelchair_accessible=2 is deleted and recreated")
    }
    else {
      HadoopConnection.fs.mkdirs(filePath)
      println(" tem_enriched_trip/wheelchair_accessible=2 is recreated")
    }
  }

  def createCSVFile(path: Path,table: Iterable[Any]): Any = {
    val stream: FSDataOutputStream = HadoopConnection.fs.create(path)
    val outputFile = new BufferedWriter(new OutputStreamWriter(stream))
    val csvWriter = new CSVWriter(outputFile, ',', CSVWriter.NO_QUOTE_CHARACTER)
    //val ListEnrichedTrips: String = csvFields.mkString(",") + "\n" + enrichedTripsTable1.mkString("\n")
    val resultCSV: String = table.mkString("\n")
      .replace("(", "")
      .replace(")", "")
    csvWriter.writeNext(resultCSV)

    println("Created enriched_tripsPARTITION1.csv with: " + table.size + " records")
  }
}
