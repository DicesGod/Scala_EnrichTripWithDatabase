package ca.mcit.option1

import java.io.File
import java.net.URL
import better.files._
import ca.mcit.model.HadoopConnection
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

class FileManagement {
  //Download and unzip the gtfs.stm
  def downloadFile: Any = {
    try {
      //Download the file from stm.info
      FileUtils.copyURLToFile(new URL("http://stm.info/sites/default/files/gtfs/gtfs_stm.zip"),
        new File("/Users/minhle/Downloads/gtfs_stm.zip"))
      println("Downloaded gtfs_stm.zip to local")

      //Unzip the file
      val zipFile: better.files.File = file"/Users/minhle/Downloads/gtfs_stm.zip"
      zipFile.unzipTo(destination = file"/Users/minhle/Downloads/gtfs_stm")
      println("Unzipped gtfs_stm.zip")
    }
    catch {
      case _: Exception => println("Connection error!")
    }
  }

  //Create directory on HDFS
  def createDirectory: Any = {
    try {
      var filePath = new Path("/user/fall2019/minhle/project4")
      if (HadoopConnection.fs.exists(filePath)) {
        println("DATA PIPELINE INSTALLATION: ")
        println(" project 4 directory already exists")
      }
      else {
        println(" project 4 directory is created")
        HadoopConnection.fs.mkdirs(filePath)
      }

      filePath = new Path("/user/fall2019/minhle/project4/trips")
      if (HadoopConnection.fs.exists(filePath)) {
        println(" trips directory already exists")
      }
      else {
        println(" trips directory is created")
        HadoopConnection.fs.mkdirs(filePath)
      }

      filePath = new Path("/user/fall2019/minhle/project4/calendar_dates")
      if (HadoopConnection.fs.exists(filePath)) {
        println(" calendar_dates directory already exists")
      }
      else {
        println(" calendar_dates directory is created")
        HadoopConnection.fs.mkdirs(filePath)
      }

      filePath = new Path("/user/fall2019/minhle/project4/frequencies")
      if (HadoopConnection.fs.exists(filePath)) {
        println(" frequencies directory already exists")
      }
      else {
        println(" frequencies directory is created")
        HadoopConnection.fs.mkdirs(filePath)
      }
    }
    catch {
      case _: Exception => println("Connection error!")
    }
  }

    //uploadFile to HDFS
  def uploadFile: Any = {
    HadoopConnection.fs.delete(new Path("/user/fall2019/minhle/project4/trips/trips.txt"), true)
    println("\nDeleted the current trips.txt file on HDFS")
    HadoopConnection.fs.copyFromLocalFile(new Path("/Users/minhle/Downloads/gtfs_stm/trips.txt"),
      new Path("/user/fall2019/minhle/project4/trips"))
    println("Uploaded the new trips.txt file to HDFS")

    HadoopConnection.fs.delete(new Path("/user/fall2019/minhle/project4/calendar_dates/calendar_dates.txt"), true)
    println("Deleted the current calendar_dates.txt file on HDFS")
    HadoopConnection.fs.copyFromLocalFile(new Path("/Users/minhle/Downloads/gtfs_stm/calendar_dates.txt"),
      new Path("/user/fall2019/minhle/project4/calendar_dates"))
    println("Uploaded the new calendar_dates.txt file to HDFS")

    HadoopConnection.fs.delete(new Path("/user/fall2019/minhle/project4/frequencies/frequencies.txt"), true)
    println("Deleted the current frequencies.txt file on HDFS")
    HadoopConnection.fs.copyFromLocalFile(new Path("/Users/minhle/Downloads/gtfs_stm/frequencies.txt"),
      new Path("/user/fall2019/minhle/project4/frequencies"))
    println("Uploaded the new frequencies.txt file to HDFS")
  }
}

