package ca.mcit.option1

import java.sql.{Connection, DriverManager, Statement}

class DatabaseDeployment{
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000/fall2019_minhle;user=minhle;password=minhle")
  val stmt: Statement = connection.createStatement()
  stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")

  def createDatabase: Any = {
    var checkDB = true
    val rs = stmt.executeQuery("SHOW DATABASES")
    while ( {
      rs.next()
    }) if (rs.getString(1).contains("fall2019_minhle")) checkDB = true
    else {checkDB = false}
    try {
      if (!checkDB) {
        stmt.execute("\nCREATE DATABASE fall2019_minhle")
      }
    }
    catch {
      case _: Exception => println("\nDatabase fall2019_minhle already exists")
    }
  }

    def createExternalTables: Any = {
      stmt.execute("DROP TABLE IF EXISTS fall2019_minhle.trips")
      stmt.execute("CREATE EXTERNAL TABLE fall2019_minhle.trips (route_id INT, " +
        "service_id STRING," +
        "trip_id STRING," +
        "trip_headsign STRING," +
        "direction_id INT," +
        "shape_id INT," +
        "wheelchair_accessible INT," +
        "note_fr STRING," +
        "note_en STRING)" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE " +
        "LOCATION '/user/fall2019/minhle/project4/trips'"+
        "TBLPROPERTIES(\"skip.header.line.count\" = \"1\")" )
      println("\nCreated external table trips in fall2019_minhle database")

      stmt.execute("DROP TABLE IF EXISTS fall2019_minhle.calendar_dates")
      stmt.execute("CREATE EXTERNAL TABLE fall2019_minhle.calendar_dates (service_id STRING, " +
        "date STRING, " +
        "exception_type INT)" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE " +
        "LOCATION '/user/fall2019/minhle/project4/calendar_dates'"+
        "TBLPROPERTIES(\"skip.header.line.count\" = \"1\")" )
      println("Created external table calendar_dates in fall2019_minhle database")

      stmt.execute("DROP TABLE IF EXISTS fall2019_minhle.frequencies")
      stmt.execute("CREATE EXTERNAL TABLE fall2019_minhle.frequencies (trip_id STRING, " +
        "start_time STRING  ," +
        "end_time STRING ," +
        "headway_secs INT)" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE " +
        "LOCATION '/user/fall2019/minhle/project4/frequencies'"+
        "TBLPROPERTIES(\"skip.header.line.count\" = \"1\")" )
      println("Created external table frequencies in fall2019_minhle database")
    }

  def createEnrichedTripTable: Any = {
    stmt.execute("DROP TABLE IF EXISTS fall2019_minhle.enriched_trip")
    stmt.execute("CREATE TABLE fall2019_minhle.enriched_trip (route_id INT, " +
      "service_id STRING," +
      "trip_id STRING," +
      "trip_headsign STRING," +
      "direction_id INT," +
      "shape_id INT," +
      "note_fr STRING," +
      "note_en STRING," +
      "date STRING, " +
      "exception_type INT," +
      "start_time STRING  ," +
      "end_time STRING ," +
      "headway_secs INT)" +
      "PARTITIONED BY (wheelchair_accessible INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "STORED AS PARQUET")
    println("Created table enriched_trip in fall2019_minhle database")
  }

  //for Option 2
  def createTempEnrichedTripTable: Any = {
    stmt.execute("DROP TABLE IF EXISTS fall2019_minhle.tem_enriched_trip")
    stmt.execute("CREATE TABLE fall2019_minhle.tem_enriched_trip (route_id INT, " +
      "service_id STRING," +
      "trip_id STRING," +
      "trip_headsign STRING," +
      "direction_id INT," +
      "shape_id INT," +
      "note_fr STRING," +
      "note_en STRING," +
      "date STRING, " +
      "exception_type INT," +
      "start_time STRING  ," +
      "end_time STRING ," +
      "headway_secs INT)" +
      "PARTITIONED BY (wheelchair_accessible INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    println("Created table tem_enriched_trip in fall2019_minhle database")
  }
}

