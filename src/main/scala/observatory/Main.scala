package observatory

import java.time.LocalDate
import org.apache.log4j.{Level, Logger}

object Main extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val records: Iterable[(LocalDate, Location, Temperature)] =
    Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")

  val averageRecords: Iterable[(Location, Temperature)] = Extraction.locationYearlyAverageRecords(records)

  val predictedTemps: Temperature = Visualization.predictTemperature(averageRecords, new Location(13.067, 80.2))

  val predictedTempColor: Iterable[(Temperature, Color)] =
    averageRecords.map {
      case (location, temperature) =>
        (temperature, Visualization.interpolateColor(Visualization.tempColorData, temperature))
    }

  val predictedColor: Color = Visualization.interpolateColor(predictedTempColor, 35d)

  println(records.take(10).map(x => x.toString() + "\n"))
  println(averageRecords.take(10).map(x => x.toString() + "\n"))
}
