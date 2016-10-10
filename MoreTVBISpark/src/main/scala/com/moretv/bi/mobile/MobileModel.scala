package com.moretv.bi.mobile

import java.io.{File, PrintWriter}
import java.net.URLDecoder

import com.moretv.bi.util.SparkSetting
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext

/**
 * Created by Will on 2015/5/11.
 */
object MobileModel extends BaseClass {

  val regex = "series=(.+?)&".r
  val lineSeparator = "\n"

  def main(args: Array[String]) {
    config.setAppName("MobileModel")
    ModuleClass.executor(MobileModel,args)
  }
  override def execute(args: Array[String]) {

    val logRdd = sc.textFile(args(0))
    val models = logRdd.map(matchLog).filter(_ != null).distinct().collect()

    val out = new PrintWriter("/home/moretv/mobileModels/"+args(1),"UTF-8")
    models.foreach({
      x =>
        out.print(x+lineSeparator)
    })

    out.close()
  }

  def matchLog(log:String) = {
    val mathcer = regex findFirstMatchIn log
    mathcer match {
      case Some(x) => URLDecoder.decode(x.group(1),"UTF-8")
      case _ => null
    }
  }
}
