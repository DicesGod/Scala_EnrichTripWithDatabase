package ca.mcit.option2

import java.sql.{Connection, DriverManager}

//Use the project of course 2 and 3 and customize it to write the files under proper partition folder on HDFS.
// Then use MSCK REPAIR command to recover partitions. The latter can be done using Hive JDBC.
object Option2 extends App {
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000/fall2019_minhle;user=minhle;password=minhle")
  val stmt = connection.createStatement()

  stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
  stmt.execute("MSCK REPAIR TABLE tem_enriched_trip")
  stmt.execute("INSERT OVERWRITE TABLE fall2019_minhle.enriched_trip PARTITION (wheelchair_accessible) SELECT route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,note_fr,note_en, " +
    "date, exception_type,start_time, end_time, headway_secs,wheelchair_accessible " +
    "FROM fall2019_minhle.tem_enriched_trip")
  stmt.execute("DROP TABLE tem_enriched_trip")
}
