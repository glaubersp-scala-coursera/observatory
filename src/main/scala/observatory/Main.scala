package observatory

import java.time.LocalDate
import org.apache.log4j.{Level, Logger}

object Main extends App {
  val records: Iterable[(LocalDate, Location, Temperature)] =
    Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")

  val averageRecords: Iterable[(Location, Temperature)] = Extraction.locationYearlyAverageRecords(records)

  println(records.take(10).map(x => x.toString() + "\n"))
  println(averageRecords.take(10).map(x => x.toString() + "\n"))
}
