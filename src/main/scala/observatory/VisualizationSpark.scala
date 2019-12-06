//package observatory
//
//import com.sksamuel.scrimage.Image
//import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
//import org.apache.spark.ml.{Pipeline, PipelineModel}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//import scala.language.postfixOps
//
///**
//  * 2nd milestone: basic visualization
//  */
//object VisualizationSpark {
//
//  val tempColorData: Seq[TempColor] = Seq(
//    TempColor(60, 255, 255, 255),
//    TempColor(32, 255, 0, 0),
//    TempColor(12, 255, 255, 0),
//    TempColor(0, 0, 255, 255),
//    TempColor(-15, 0, 0, 255),
//    TempColor(-27, 255, 0, 255),
//    TempColor(-50, 33, 0, 107),
//    TempColor(-60, 0, 0, 0)
//  )
//
//  val tempColorData2: Seq[(Temperature, Color)] = Seq(
//    (60, Color(255, 255, 255)),
//    (32, Color(255, 0, 0)),
//    (12, Color(255, 255, 0)),
//    (0, Color(0, 255, 255)),
//    (-15, Color(0, 0, 255)),
//    (-27, Color(255, 0, 255)),
//    (-50, Color(33, 0, 107)),
//    (-60, Color(0, 0, 0))
//  )
//
//  val spark: SparkSession =
//    SparkSession
//      .builder()
//      .appName("Observatory")
//      .master("local[4]")
//      .getOrCreate()
//
//  // For implicit conversions like converting RDDs to DataFrames
//  import spark.implicits._
//
//  /**
//    * This method takes a sequence of known temperatures at the given locations, and a location where we want to guess the temperature, and returns an estimate based on the inverse distance weighting algorithm.
//    * (you can use any p value greater or equal to 2; try and use whatever works best for you!).
//    * To approximate the distance between two locations, we suggest you to use the great-circle distance formula.
//    *
//    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
//    * @param location Location where to predict the temperature
//    * @return The predicted temperature at `location`
//    */
//  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
//    val elements: RDD[(Boolean, (Distance, (Temperature)))] = spark.sparkContext
//      .parallelize[(Location, Temperature)](temperatures.toSeq)
//      .map {
//        case (locationI, temperature) =>
//          val dist = d(location, locationI)
//          if (dist == 0)
//            (true, (dist, (temperature)))
//          else
//            (false, (dist, (temperature)))
//      }
//      .cache()
//
//    if (elements.filter(p => p._1).count() > 0) {
//      elements.filter(p => p._1).take(1).head._2._2
//    } else {
//      val pairs = elements
//        .filter(p => !p._1)
//        .aggregate[(Double, Double)]((0, 0))(
//          {
//            case ((acc1, acc2), item) => {
//              val dist = item._2._1
//              val temp = item._2._2
//              val wi = (1.0 / Math.pow(dist, 2))
//              (acc1 + wi * temp, acc2 + wi)
//            }
//          }, { case ((a, b), (c, d)) => (a + c, b + d) }
//        )
//      pairs._1 / pairs._2
//    }
//  }
//
//  private def w_i(location: Location, locationI: Location): Distance = {
//    1.0 / Math.pow(d(location, locationI), 2)
//  }
//
//  private def d(location: Location, locationI: Location): Distance = {
//    6731 * deltaSigma(location, locationI)
//  }
//
//  private def deltaSigma(loc: Location, locI: Location): Distance = {
//    def antipodes(location: Location, location1: Location): Boolean = {
//      (Math.abs(location.lon) == Math.abs(location1.lon)) &&
//      (Math.abs(location.lat) == Math.abs(location1.lat))
//    }
//
//    if (loc.canEqual(locI)) 0
//    else if (antipodes(loc, locI)) Math.PI
//    else {
//      Math.acos(
//        Math.sin(loc.lat) * Math.sin(locI.lat) +
//          Math.cos(loc.lat) * Math.cos(locI.lat) *
//            Math.cos(Math.abs(loc.lon - locI.lon))
//      )
//    }
//  }
//
//  /**
//    * This method takes a sequence of reference temperature values and their associated color,
//    * and a temperature value, and returns an estimate of the color corresponding to the given value,
//    * by applying a linear interpolation algorithm.
//    *
//    * Note that the given points are not sorted in a particular order.
//    *
//    * @param points Pairs containing a value and its associated color
//    * @param value The value to interpolate
//    * @return The color that corresponds to `value`, according to the color scale defined by `points`
//    */
//  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
//
//    val data = spark.sparkContext
//      .parallelize[TempColor](points.map {
//        case (temperature, color) =>
//          TempColor(temperature, color.red, color.green, color.blue)
//      }.toSeq)
//      .toDF()
//      .cache()
//
//    val featuresVector = new VectorAssembler()
//      .setInputCols(Array("temp"))
//      .setOutputCol("features")
//
//    // Define model to use
//    val lrRed = new LinearRegression().setLabelCol("red").setPredictionCol("redPrediction")
//    val lrGreen = new LinearRegression().setLabelCol("green").setPredictionCol("greenPrediction")
//    val lrBlue = new LinearRegression().setLabelCol("blue").setPredictionCol("bluePrediction")
//
//    // Create a pipeline that associates the model with the data processing sequence
//    val pipeline = new Pipeline().setStages(Array(featuresVector, lrRed, lrGreen, lrBlue))
//
//    val model: PipelineModel = pipeline.fit(data)
//
//    val linRegModel = model.stages(1).asInstanceOf[LinearRegressionModel]
//    println(linRegModel.coefficients)
//
//    val prediction: DataFrame =
//      model
//        .transform(Seq(TempColor(value, 0, 0, 0)).toDF())
//
//    val result = prediction
//      .map { row =>
//        Color(
//          row.getAs[Double]("redPrediction").round.intValue(),
//          row.getAs[Double]("greenPrediction").round.intValue(),
//          row.getAs[Double]("bluePrediction").round.intValue()
//        )
//      }
//    result.head
//  }
//
//  /**
//    * @param temperatures Known temperatures
//    * @param colors Color scale
//    * @return A 360×180 image where each pixel shows the predicted temperature at its location
//    */
//  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
//    ???
//  }
//
//}
