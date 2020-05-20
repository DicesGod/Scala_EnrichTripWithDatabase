package ca.mcit.option1

import java.sql.{Connection, DriverManager}

//Implement a Scala/Java application and run the SQL query using Hive JDBC
object Option1 extends App{
  try {
    val filemanagement = new FileManagement
    filemanagement.downloadFile
    filemanagement.createDirectory
    filemanagement.uploadFile

    val databaseDeployment = new DatabaseDeployment
    databaseDeployment.createDatabase
    databaseDeployment.createExternalTables
    databaseDeployment.createEnrichedTripTable

    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driverName)

    val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000/fall2019_minhle;user=minhle;password=minhle")
    val stmt = connection.createStatement()
    stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
    stmt.execute("INSERT OVERWRITE TABLE enriched_trip PARTITION (wheelchair_accessible)" +
      "SELECT /*+ STREAMTABLE(t) */ t.route_id,t.service_id,t.trip_id,t.trip_headsign,t.direction_id," +
      "t.shape_id,t.note_fr,t.note_en,c.date,c.exception_type,f.start_time," +
      "f.end_time,f.headway_secs,t.wheelchair_accessible " +
      "FROM trips t " +
      "LEFT JOIN calendar_dates c on c.service_id =t.service_id " +
      "LEFT JOIN frequencies f on f.trip_id = t.trip_id")
    println("Inserted data into enrich_trip table")
    //239461
    stmt.close()
    connection.close()
  }
  catch
    {
      case _: Exception => println("\nConnection error! Please try again later!")
    }
}
