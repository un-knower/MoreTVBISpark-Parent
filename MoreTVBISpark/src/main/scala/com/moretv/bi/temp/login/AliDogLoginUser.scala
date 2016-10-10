package com.moretv.bi.temp.login


import com.moretv.bi.util._
import org.apache.spark.SparkContext
import FileUtils._

/**
 * Created by Will on 2015/2/5.
 */
object AliDogLoginUser extends SparkSetting{

  val aliDog = "MoreTV_TVApp2.0_Android_YunOS2_"
  val tvDog = "MoreTV_TVApp2.0_Android_YunOS_"

  def main(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "2g").
          set("spark.cores.max", "30").
          set("spark.storage.memoryFraction", "0.6")

        val sc = new SparkContext(config)

        val inputDate =  p.startDate
        val logRDD = sc.textFile(s"/log/loginlog/loginlog.access.log_$inputDate-portalu*").
          map(matchLog).filter(_ != null).cache()

        val userNum = logRDD.distinct().countByKey()

        logRDD.unpersist()
        sc.stop()
        val day = DateFormatUtils.toDateCN(inputDate,-1)

        withCsvWriter(s"TVDogLoginUser-$day.csv"){
          out => {
            userNum.foreach(t => {
              if(t._1._1 == tvDog){
                out.println(s"${t._1._1},${t._1._2},${t._2}")
              }
            })
          }
        }
        withCsvWriter(s"AliDogLoginUser-$day.csv"){
          out => {
            userNum.foreach(t => {
              if(t._1._1 == aliDog){
                out.println(s"${t._1._1},${t._1._2},${t._2}")
              }
            })
          }
        }
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

  def matchLog(log:String) ={
    val json = LogUtils.log2jsonLite(log)
    if(json != null) {
      val mac = json.optString("mac")
      val version = json.optString("version")
      val productModel = json.optString("ProductModel")
      if(version.startsWith(aliDog)){
        ((aliDog,productModel),mac)
      }else if(version.startsWith(tvDog)){
        ((tvDog,productModel),mac)
      }else null
    }else null
  }

}
