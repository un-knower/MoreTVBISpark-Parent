package com.moretv.bi.report.medusa.liveCastStat

/**
  * Created by witnes on 1/13/17.
  */
object LivePathMatch {

  val live_First_Category_Str = LiveSationTree.Live_First_Category.mkString("|")

  val categoryMatch = (s: String) => {

    val re = s"(${live_First_Category_Str})".r

    re findFirstMatchIn s match {
      case Some(p) => p.group(1)
      case None => LiveSationTree.others
    }
    LiveSationTree.others
  }

}
