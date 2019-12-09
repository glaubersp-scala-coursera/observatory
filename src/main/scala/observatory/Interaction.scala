package observatory

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {

  /**
    * This method converts a tile's geographic position to its corresponding GPS coordinates,
    * by applying the Web Mercator projection.
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Scala
    */
  def tileLocation(tile: Tile): Location = new Location(
    Math.toDegrees(Math.atan(Math.sinh(Math.PI * (1.0 - 2.0 * tile.y.toDouble / (1 << tile.zoom))))),
    tile.x.toDouble / (1 << tile.zoom) * 360.0 - 180.0
  )

  /**
    * This method returns a 256×256 image showing the given temperatures, using the given color
    * scale, at the location corresponding to the given zoom, x and y values. Note that the pixels
    * of the image must be a little bit transparent so that when we will overlay the tile on the
    * map, the map will still be visible. We recommend using an alpha value of 127.
    *
    * Hint: you will have to compute the corresponding latitude and longitude of each pixel within
    * a tile. A simple way to achieve that is to rely on the fact that each pixel in a tile can be
    * thought of a subtile at a higher zoom level (256 = 2⁸).
    *
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)],
           colors: Iterable[(Temperature, Color)],
           tile: Tile): Image = {
    val zoom = tile.zoom + 1
    val minX = tile.x - 128
    val minY = tile.y - 128

    val tiles = for {
      x <- minX to minX + 256
      y <- minY to minY + 256
    } yield Tile(x, y, zoom)

    val pixels = tiles
      .map(t => tileLocation(t))
      .map(l => Visualization.predictTemperature(temperatures, l))
      .map(temp => Visualization.interpolateColor(colors, temp))
      .map(c => Pixel(c.red, c.green, c.blue, 127))
      .toArray
    Image(256, 256, pixels)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
      yearlyData: Iterable[(Year, Data)],
      generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    for {
      (year, data) <- yearlyData
      zoom <- 0 to 3
      x <- 0 until 1 << zoom
      y <- 0 until 1 << zoom
    } yield generateImage(year, Tile(x, y, zoom), data)
  }

}
