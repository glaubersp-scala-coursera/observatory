package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import org.apache.log4j.{Level, Logger}

import scala.collection._
import scala.language.postfixOps

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val tempColorData: Seq[(Temperature, Color)] = Seq(
    (60, Color(255, 255, 255)),
    (32, Color(255, 0, 0)),
    (12, Color(255, 255, 0)),
    (0, Color(0, 255, 255)),
    (-15, Color(0, 0, 255)),
    (-27, Color(255, 0, 255)),
    (-50, Color(33, 0, 107)),
    (-60, Color(0, 0, 0))
  )

  /**
    * This method takes a sequence of known temperatures at the given locations, and a location where we want to guess the temperature, and returns an estimate based on the inverse distance weighting algorithm.
    * (you can use any p value greater or equal to 2; try and use whatever works best for you!).
    * To approximate the distance between two locations, we suggest you to use the great-circle distance formula.
    *
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val elements = temperatures
      .map {
        case (locationI, temperature) =>
          val dist = d(location, locationI)
          if (dist <= 1.0)
            (true, (dist, (temperature)))
          else
            (false, (dist, (temperature)))
      }

    if (elements.exists(p => p._1)) {
      elements.filter(p => p._1).take(1).head._2._2
    } else {
      val pairs = elements
        .filter(p => !p._1)
        .aggregate[(Double, Double)]((0, 0))(
          {
            case ((acc1, acc2), item) =>
              val dist = item._2._1
              val temp = item._2._2
              val wi = (1.0 / Math.pow(dist, 3))
              (acc1 + wi * temp, acc2 + wi)
          }, { case ((a, b), (c, d)) => (a + c, b + d) }
        )
      pairs._1 / pairs._2
    }
  }

  private def d(location: Location, locationI: Location): Distance = {
    6731 * deltaSigma(location, locationI)
  }

  private def deltaSigma(loc: Location, locI: Location): Distance = {
    def antipodes(location: Location, location1: Location): Boolean = {
      (Math.abs(location.lon) == Math.abs(location1.lon)) &&
      (Math.abs(location.lat) == Math.abs(location1.lat))
    }

    if (loc.canEqual(locI)) Math.toRadians(0)
    else if (antipodes(loc, locI)) Math.toRadians(Math.PI)
    else {
      Math.acos(
        Math.sin(Math.toRadians(loc.lat)) * Math.sin(Math.toRadians(locI.lat)) +
          Math.cos(Math.toRadians(loc.lat)) * Math.cos(Math.toRadians(locI.lat)) *
            Math.cos(Math.abs(Math.toRadians(loc.lon) - Math.toRadians(locI.lon)))
      )
    }
  }

  /**
    * This method takes a sequence of reference temperature values and their associated color,
    * and a temperature value, and returns an estimate of the color corresponding to the given value,
    * by applying a linear interpolation algorithm.
    *
    * Note that the given points are not sorted in a particular order.
    *
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    points.find(p => p._1 == value) match {
      case Some(x @ (t, c)) => c
      case None =>
        val (less, more) = points.toIndexedSeq
          .sortBy(p => p._1)
          .partition(p => p._1 <= value)
        interpolateColorForTemperature(less.lastOption, more.headOption, value)
    }
  }

  private def interpolateColorForTemperature(pair1: Option[(Temperature, Color)],
                                             pair2: Option[(Temperature, Color)],
                                             temp: Temperature): Color = {

    (pair1, pair2) match {
      case (Some((t1, Color(r1, g1, b1))), Some((t2, Color(r2, g2, b2)))) =>
        val r = interpolate(t1, r1, t2, r2, temp)
        val g = interpolate(t1, g2, t2, g2, temp)
        val b = interpolate(t1, b1, t2, b2, temp)
        Color(r, g, b)
      case (Some((t1, c1)), None) => c1
      case (None, Some((t2, c2))) => c2
      case (_, _)                 => Color(0, 0, 0)
    }
  }

  private def interpolate(t1: Temperature, c1: Int, t2: Temperature, c2: Int, t: Temperature): Int = {
    (c1 + (c2 - c1) * ((t - t1) / (t2 - t1))).round.toInt
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val pixels: Array[Pixel] = temperatures
      .map { lt =>
        val pos = getPos(lt._1)
        val temp = lt._2
        val color = interpolateColor(colors, lt._2)
        (pos, temp, color)
      }
      .toIndexedSeq
      .sortBy(triple => triple._1)
      .map(triple => {
        val color = triple._3
        Pixel(color.red, color.green, color.blue, 100)
      })
      .toArray
    Image(360, 180, pixels)
  }

  def getPos(loc: Location): Point = {
    val x = (360.0 / 359.0) * (loc.lat + 180.0)
    val y = -(180.0 / 179.0) * (loc.lon - 90.0)
    Point(x, y)
  }
}
