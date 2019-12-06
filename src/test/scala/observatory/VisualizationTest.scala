package observatory

import org.scalacheck.Gen
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.prop.{Checkers, GeneratorDrivenPropertyChecks}

trait VisualizationTest extends FunSuite with Checkers with Matchers with GeneratorDrivenPropertyChecks {
  val colorGen = for {
    r <- Gen.choose(0, 255)
    g <- Gen.choose(0, 255)
    b <- Gen.choose(0, 255)
  } yield Color(r, g, b)

  test("Location.canEqual") {
    val loc1 = Location(0.2, 1.4)
    val loc2 = Location(0.2, 1.4)
    val loc3 = Location(0.3, 1.4)
    assert(loc1.canEqual(loc2) && !loc1.canEqual(loc3))
  }

  test("interpolateColor") {
    assert(
      Visualization
        .interpolateColor(List((-1.0, Color(255, 0, 0)), (0.0, Color(0, 0, 255))), -0.5) == Color(128, 0, 128))

  }

//  test("exceeding the greatest value of a color scale should return the color associated with the greatest value") {
//    check((t1: Temperature, t2: Temperature, t3: Temperature, c1: Color, c2: Color, r: Int, g: Int, b: Int) =>
//      whenever(r <=255 && g <= 255 && b <= 255)(Visualization.interpolateColorForTemperature(None, Some(t1, c1), t3) == Color(r, g, b)))
//
//  }

//  test("predictTemperature"){
//    val loc = Location(88.0,-176.0)
//    val temp = Visualization.predictTemperature()
//      : 17.320031242356592. Expected to be closer to 10.0 than 20.0
//  }

}
