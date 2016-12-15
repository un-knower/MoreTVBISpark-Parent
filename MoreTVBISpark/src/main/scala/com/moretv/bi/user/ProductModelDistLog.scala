package com.moretv.bi.user

import java.util.Date

import com.moretv.bi.util.FileUtils._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{LogUtils, ProductModelUtil, SparkSetting}
import org.apache.spark.SparkContext

/**
  * Created by Will on 2015/8/5.
  */
object ProductModelDistLog extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(ProductModelDistLog,args)
  }
   override def execute(args: Array[String]) {
     val logRdd = sc.textFile("/log/loginlog/loginlog.access.log_"+args(0)+"*").map(matchLog).
       filter(_ != null).distinct()

     val modelResult = logRdd.countByKey()
     val brandResult = logRdd.map(x => (ProductModelUtil.getProductBrand(x._1),x._2)).countByKey()

     withCsvWriterOld("/home/moretv/liankai.tmp/share_dir/productModelDistLog.csv"){
       out => {
         out.println(new Date())
         modelResult.foreach(x => {
           out.println(x._1+","+x._2)
         })
       }
     }

     withCsvWriterOld("/home/moretv/liankai.tmp/share_dir/productBrandDistLog.csv"){
       out => {
         out.println(new Date())
         brandResult.foreach(x => {
           out.println(x._1+","+x._2)
         })
       }
     }

   }

  def matchLog(log: String) = {
    val json = LogUtils.log2json(log)
    if(json != null){
      val version = json.optString("version","222222222")
      val length = version.length
      if(version.substring(length - 5) == "2.5.7") {
        (json.optString("ProductModel"),json.optString("userId"))
      } else null
    } else null

  }

 }
