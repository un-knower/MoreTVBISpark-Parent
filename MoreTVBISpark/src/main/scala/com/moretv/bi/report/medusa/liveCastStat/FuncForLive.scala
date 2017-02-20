package com.moretv.bi.report.medusa.liveCastStat


/**
  * Created by witnes on 2/16/17.
  */
object FuncForLive {


  var step = 10


  /**
    * fill the gap between the start time and the end time with with specified step
    * both start and end time is defined by its corresponding hour and minute
    *
    * @param startHour
    * @param endHour
    * @param startMinute
    * @param endMinute
    * @return
    */
  def periodFillingWithStartEnd(startHour: Int, endHour: Int,
                                startMinute: Int, endMinute: Int): Seq[(Int, Int)] = {

    val startMinuteF = startMinute / step * step
    val endMinuteF = endMinute / step * step

    ((startHour :: Nil).cross(startMinuteF until 60 by step)).toList ++
      ((startHour + 1 to endHour - 1 by 1).cross(0 until 60 by step)).toList ++
      ((endHour :: Nil).cross(0 to endMinuteF by step)).toList

  }


  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for {x <- xs; y <- ys} yield (x, y)
  }


}
