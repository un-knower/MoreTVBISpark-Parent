package com.moretv.bi.temp

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext

/**
 * Created by Will on 2015/2/5.
 */
object NestLive extends SparkSetting{

  val regex = "log=homerecommend-001-access-[\\w\\.]+-([0-9a-z]{32})-.+-live-s9n8op9wxyab-".r

  def main(args: Array[String]) {

    val sc = new SparkContext(config)
    val inputDate = args(0)
    val logRDD = sc.textFile(s"/log/data/moretvlog.access.log-$inputDate*").map(matchLog).
      filter(_ != null)

    val times = logRDD.count()
    val user = logRDD.distinct().count()

    println("#########################################")
    println(s"user-times:$user    $times")
    println("#########################################")
  }

  def matchLog(log:String) ={
    regex findFirstMatchIn log match {
      case Some(m) => m.group(1)
      case None => null
    }
  }

}
