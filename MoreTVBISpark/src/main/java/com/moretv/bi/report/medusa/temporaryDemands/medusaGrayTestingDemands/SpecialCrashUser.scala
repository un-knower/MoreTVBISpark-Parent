package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

/**
 * Created by xiajun on 2016/3/28.
 * 该对象用于获取每天的crash的原始数据
 */
import java.lang.{Long => JLong}

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util.{ParamsParseUtil, SparkSetting, _}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json.JSONObject


object SpecialCrashUser extends SparkSetting{
  val sc = new SparkContext(config)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  val util = new DBOperationUtils("medusa")


  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate)
        val logRdd = sc.textFile(s"/log/crash/metadata/${inputDate}_extraction.log").map(log=>{
          val json = new JSONObject(log)
          (json.optString("fileName"),json.optString("MAC"),json.optString("APP_VERSION_NAME"),json.optString("APP_VERSION_CODE"),
            json.optString("ANDROID_VERSION"),json.optString("STACK_TRACE"),json.optString("DATE_CODE"),json.optString("PRODUCT_CODE"))
        })

        val filterRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log._7,log._8))
          .filter(data => !DevMacUtils.macFilter(data._2))

        /**
         * Create a new DF which including the md5
         */
        filterRdd.map(e=>(e._1,DigestUtils.md5Hex(e._1),e._2,e._3,e._4,e._5,e._6,DigestUtils.md5Hex(e._6),e._7,e._8)).
          toDF("fileName","fileName_md5","mac","appVersionName","appVersionCode","androidVersion","stackTrace","stackTraceMD5",
          "dateCode","productCode").registerTempTable("crashInfo_MD5")

        /**
         * The statistic process for different needs
         *
         */

        val fileName = sqlContext.sql("select fileName from crashInfo_MD5 where " +
          "stackTraceMD5='000e75002304ed2d6fe0b18de91b2a52'").map(e=>(e.getString(0)))
        val insert_sql = "insert into temp5(file) values (?)"
        fileName.foreach(e=>{
          try{
            util.insert(insert_sql,e)
          }catch{
            case e:Exception=> e.printStackTrace()
          }

        })
      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}

