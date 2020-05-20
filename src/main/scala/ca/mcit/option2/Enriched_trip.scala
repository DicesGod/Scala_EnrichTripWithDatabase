package ca.mcit.option2

import org.apache.hadoop.fs.Path
import ca.mcit.option1.DatabaseDeployment

object Enriched_trip extends App {
  try {
    val databaseDeployment = new DatabaseDeployment
    databaseDeployment.createEnrichedTripTable
    databaseDeployment.createTempEnrichedTripTable

    val fileManagement = new FileManagement
    fileManagement.createPartitionFolder

    val lookupTripMap: Map[(String,String), Trip] = Trip.createTripsMap
    val lookupCalendar_datesMap: Map[String, Calendar_date] = Calendar_date.createCalendar_datesMap
    val lookupFrequenciesMap: Map[String, Frequency] = Frequency.createFrequenciesMap

    //Join the Trips,Calendar_dates and Frequencies
    val enrichedTripsTable1 = lookupTripMap.collect { case ((service_id, trip_id), trip)
      if trip.wheelchair_accessible == 1 => (trip.route_id,trip.service_id,trip.trip_id,trip.trip_headsign,trip.direction_id,trip.shape_id,
      trip.note_fr,trip.note_en, lookupCalendar_datesMap.get(service_id), lookupFrequenciesMap.get(trip_id),trip.wheelchair_accessible)
    }

    val enrichedTripsTable2 = lookupTripMap.collect{ case ((service_id,trip_id),trip)
      if trip.wheelchair_accessible == 2 => (trip.route_id,trip.service_id,trip.trip_id,trip.trip_headsign,trip.direction_id,trip.shape_id,
      trip.note_fr,trip.note_en, lookupCalendar_datesMap.get(service_id), lookupFrequenciesMap.get(trip_id),trip.wheelchair_accessible)
      //    lookupCalendar_datesMap.get(service_id).get.date,lookupCalendar_datesMap.get(service_id).get.exception_type,
      //    lookupFrequenciesMap.get(trip_id).get.start_time,lookupFrequenciesMap.get(trip_id).get.end_time,
      //    lookupFrequenciesMap.get(trip_id).get.headway_secs)
    }

    var filePath = new Path("/user/hive/warehouse/fall2019_minhle.db/tem_enriched_trip/wheelchair_accessible=1/enriched_tripsPARTITION1.csv")
    fileManagement.createCSVFile(filePath,enrichedTripsTable1)

    filePath = new Path("/user/hive/warehouse/fall2019_minhle.db/tem_enriched_trip/wheelchair_accessible=2/enriched_tripsPARTITION2.csv")
    fileManagement.createCSVFile(filePath,enrichedTripsTable2)

    println("Deployment completed!")

    //Trying to create parquet file using spark
    //  val configuration = new SparkConf().setAppName("course4project").setMaster("local")
    //  val sc = new SparkContext(configuration)
    //  val spark = SparkSession
    //    .builder()
    //    .appName("course4project")
    //    .config("local", "some-value")
    //    .getOrCreate()

    //  val conf = new SparkConf().setMaster("spark://master") //missing
    //
    //  //read csv with options
    //  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"false"))
    //   // .csv("/user/hive/warehouse/fall2019_minhle.db/enriched_trip/wheelchair_accessible=2/enriched_tripsPARTITION2.csv")
    //      .csv("/Users/minhle/Downloads/enriched_tripsPARTITION2.csv")
    //
    //  //df.show()
    //  //df.printSchema()
    //  df.write.parquet("/Users/minhle/Documents/enriched_tripsPARTITION2.parquet")
    //df.write.parquet("/user/hive/warehouse/fall2019_minhle.db/enriched_trip/wheelchair_accessible=2/enriched_tripsPARTITION2.parquet")
    //HadoopConnection.fs.delete(new Path("/user/hive/warehouse/fall2019_minhle.db/enriched_trip/wheelchair_accessible=2/enriched_tripsPARTITION2.csv"),true)

    //  val df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"false"))
    //    .csv("/user/hive/warehouse/fall2019_minhle.db/enriched_trip/wheelchair_accessible=1/enriched_tripsPARTITION1.csv")
    //  df2.show()
    //  df2.printSchema()
    //  df2.write.parquet("/user/hive/warehouse/fall2019_minhle.db/enriched_trip/wheelchair_accessible=1/enriched_tripsPARTITION1.parquet")
    //  HadoopConnection.fs.delete(new Path("/user/hive/warehouse/fall2019_minhle.db/enriched_trip/wheelchair_accessible=1/enriched_tripsPARTITION1.csv"),true)
  }
  catch
    {
      case _: Exception => println("\nConnection error! Please try again later!")
    }
}

