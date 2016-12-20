package com.moretv.bi.account

import com.moretv.bi.util.SparkSetting
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import com.moretv.bi.util.FileUtils._

/**
 * Created by Will on 2015/7/29.
 */
object LoginTimes extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(LoginTimes,args)
  }
  override def execute(args: Array[String]) {

    val moretvIds = sc.textFile("/log/temp/moretvid.log").collect().toList
    val metadata = sc.broadcast(moretvIds)
    val logRdd = sc.textFile("/log/temp/account_proxy-20150729.access.log").map(matchLog).
      filter(_ != null).filter(metadata.value.contains)
    val result = logRdd.countByValue()
    withCsvWriterOld("/home/moretv/liankai.tmp/share_dir/account_20150729.csv"){
      out => {
        out.println("total,"+result.size)
        result.foreach(x => {
          out.println(x._1 + "," + x._2)
        })
      }
    }
  }

  def matchLog(log: String) = {
    val regex = "\\[(\\d{2})/Jul/2015.+/userInfo/getUserInfoByMoretvid.+moretvid=(\\d+)".r
    val matcher = regex findFirstMatchIn log
    matcher match {
      case Some(m) => {
        val day = m.group(1).toInt
        if(day >= 13 && day <= 20){
          m.group(2)
        }else null
      }
      case None => null
    }
  }

}
