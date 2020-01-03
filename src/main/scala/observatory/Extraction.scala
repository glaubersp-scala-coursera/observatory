package observatory

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.io.Source

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  import org.apache.spark.sql.SparkSession

  private val STN_ID_COL: String = "STN_identifier"
  private val WBAN_ID_COL: String = "WBAN_identifier"
  private val LATITUDE_COL: String = "Latitude"
  private val LONGITUDE_COL: String = "Longitude"
  private val MONTH_COL: String = "Month"
  private val DAY_COL: String = "Day"
  private val TEMPERATURE_COL: String = "Temperature"

  val STATION_COLUMNS: IndexedSeq[String] = IndexedSeq(STN_ID_COL, WBAN_ID_COL, LATITUDE_COL, LONGITUDE_COL)
  val STATION_HEADERS: Map[String, DataType] =
    Map(
      (STATION_COLUMNS(0), IntegerType),
      (STATION_COLUMNS(1), IntegerType),
      (STATION_COLUMNS(2), DoubleType),
      (STATION_COLUMNS(3), DoubleType)
    )
  val TEMPERATURE_COLUMNS: IndexedSeq[String] = IndexedSeq(STN_ID_COL, WBAN_ID_COL, MONTH_COL, DAY_COL, TEMPERATURE_COL)
  val TEMPERATURE_HEADERS: Map[String, DataType] =
    Map(
      (TEMPERATURE_COLUMNS(0), IntegerType),
      (TEMPERATURE_COLUMNS(1), IntegerType),
      (TEMPERATURE_COLUMNS(2), IntegerType),
      (TEMPERATURE_COLUMNS(3), IntegerType),
      (TEMPERATURE_COLUMNS(4), DoubleType)
    )

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .master("local[4]")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  private def parseRow(x: String): List[String] = {
    val string = if (x.endsWith(",")) { s"${x}0" } else x
    string.split(",").map(x => if ("".equalsIgnoreCase(x)) "0" else x).to[List]
  }

  private def stationRow(line: List[String]): Row = {
    val values = line.head.toInt :: line(1).toInt :: line(2).toDouble :: line(3).toDouble :: Nil
    Row(values: _*)
  }

  private def readStations(resource: String): DataFrame = {
    val rdd: RDD[String] = getRDDFromResource(resource)

    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(STATION_COLUMNS, STATION_HEADERS)
    val data = rdd
      .map(x => parseRow(x))
      .map(x => stationRow(x))
    val dataFrame = spark.createDataFrame(data, schema)
    dataFrame
  }

  private def convertTemperature(tempF: Double): Double = {
    val tempC = (tempF - 32d) * 5d / 9d
    tempC
  }

  private def temperatureRow(line: List[String]): Row = {
    val values = {
      if ("".equals(line.head)) 0 else line.head.toInt
    } :: {
      if ("".equals(line(1))) 0 else line(1).toInt
    } ::
      line(2).toInt ::
      line(3).toInt ::
      //      line(4).toDouble :: Nil
      convertTemperature(line(4).toDouble) :: Nil
    Row(values: _*)
  }

  def readTemperatures(resource: String): DataFrame = {
    val rdd = getRDDFromResource(resource)
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(TEMPERATURE_COLUMNS, TEMPERATURE_HEADERS)
    val data = rdd
      .map(x => parseRow(x))
      .map(x => temperatureRow(x))
    val dataFrame = spark.createDataFrame(data, schema)
    dataFrame
  }

  /**
    * Load the resource from filesystem as RDD[String]
    * @param resource the resource path
    * @return the resource content as RDD[String]
    */
  def getRDDFromResource(resource: String): RDD[String] = {
    val fileStream = Source.getClass.getResourceAsStream(resource)
    spark.sparkContext.makeRDD(Source.fromInputStream(fileStream).getLines().toList)
  }

  /** @return The schema of the DataFrame, assuming that the first given column has type String and all the others
    *         have type Double. None of the fields are nullable.
    * @param columnNamesTypesMap Column names of the DataFrame
    */
  def dfSchema(columNameList: IndexedSeq[String], columnNamesTypesMap: Map[String, DataType]): StructType = StructType(
    columNameList.map { cName =>
      StructField(cName, columnNamesTypesMap(cName), nullable = false)
    }.toSeq
  )

  /**
    *
    * This method should return the list of all the temperature records converted in degrees Celsius
    * along with their date and location (ignore data coming from stations that have no GPS coordinates).
    * You should not round the temperature values.
    * The file paths are resource paths, so they must be absolute locations in your classpath
    * (so that you can read them with getResourceAsStream).
    * For instance, the path for the resource file 1975.csv is /1975.csv.
    *
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year,
                         stationsFile: String,
                         temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    val stationsDF: DataFrame = readStations(stationsFile).persist()
    val tempDF: DataFrame = readTemperatures(temperaturesFile).persist()

    val finalDR = stationsDF
      .join(tempDF, Seq(STN_ID_COL, WBAN_ID_COL))

    val result = finalDR
      .filter($"Latitude" =!= "0" && $"Longitude" =!= "0")
      .collect()
      .map(
        (row: Row) =>
          (LocalDate.of(year, row.getInt(4), row.getInt(5)),
           Location(row.getDouble(2), row.getDouble(3)),
           row.getDouble(6)))
    result
  }

  /**
    * This method should return the average temperature at each location, over a year.
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(
      records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    sparkAverageRecords(spark.sparkContext.parallelize(records.toSeq)).collect().toSeq
  }

  /**
    * This method should return the average temperature at each location, over a year.
    * @param records RDD[(LocalDate, Location, Temperature)]
    * @return RDD[(Location, Temperature)]
    */
  private def sparkAverageRecords(
      records: RDD[(LocalDate, Location, Temperature)]
  ): RDD[(Location, Temperature)] = {
    records
      .map(r => (r._2, (r._3, 1)))
      .reduceByKey((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2))
      .mapValues { case (temp, count) => temp / count }
  }
}
