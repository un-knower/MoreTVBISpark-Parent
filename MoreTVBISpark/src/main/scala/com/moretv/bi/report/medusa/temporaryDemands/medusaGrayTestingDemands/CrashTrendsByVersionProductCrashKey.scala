package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

/**
 * Created by Administrator on 2016/3/28.
 */

import java.lang.{Long => JLong}

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json.JSONObject
import src.com.moretv.bi.medusa.util.ParquetDataStyle.ALL_CRASH_INFO

object CrashTrendsByVersionProductCrashKey extends SparkSetting{
  val sc = new SparkContext()
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
            json.optString("CRASH_KEY"),json.optString("STACK_TRACE"),json.optString("DATE_CODE"),json.optString("PRODUCT_CODE"))
        })

        // 内存溢出的crash
        val filterOutofMemoryRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log
          ._7,log._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._5.contains("java.lang." +
          "OutOfMemoryError"))
        // 空指针的crash
        val filterNullPointerRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log
          ._7,log._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._5.contains("java" +
          ".lang.NullPointerException"))
        // 所有的crash
        val filterAllRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log._7,log
          ._8)).filter(data => !DevMacUtils.macFilter(data._2))

        /**
         * Transform the RDD to DataFrame
         */

        val DF2 = filterOutofMemoryRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF2.registerTempTable("crashInfo2")

        val DF5 = filterNullPointerRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF5.registerTempTable("crashInfo5")

        val DFAll = filterAllRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,data._6,
          data._7,data._8)).toDF()
        DFAll.registerTempTable("crashInfoAll")


        /**
         * 统计不同的crashType和buildDate情况
         */
        /*query sql*/

        val versionProductOutofmemorySql = "select App_version_name,Date_code,Product_code,count(Mac),count(distinct " +
          "Mac) from crashInfo2 where App_version_name is not null and App_version_name != '' and length(App_version_name)" +
          "=5 group by App_version_name,Date_code,Product_code " +
          "union " +
          "select App_version_name,Date_code,'All' as Product_code,count(Mac),count(distinct " +
          "Mac) from crashInfo2 where App_version_name is not null and App_version_name != '' and length(App_version_name)" +
          "=5 group by App_version_name,Date_code"


        val versionProductNullpointerSql = "select App_version_name,Date_code,Product_code,count(Mac),count(distinct Mac) " +
          "from crashInfo5 where App_version_name is not null and App_version_name != '' and length(App_version_name)=5 " +
          "group by App_version_name,Date_code,Product_code " +
          "union " +
          "select App_version_name,Date_code,'All' as Product_code,count(Mac),count(distinct " +
          "Mac) from crashInfo5 where App_version_name is not null and App_version_name != '' and length(App_version_name)=5 " +
          "group by App_version_name,Date_code"

        val versionProductAllSql = "select App_version_name,Date_code,Product_code,count(Mac),count(distinct Mac) " +
          "from crashInfoAll where App_version_name is not null and App_version_name != '' and length(App_version_name)=5 " +
          "group by App_version_name,Date_code,Product_code " +
          "union " +
          "select App_version_name,Date_code,'All' as Product_code,count(Mac),count(distinct " +
          "Mac) from crashInfoAll where App_version_name is not null and App_version_name != '' and length" +
          "(App_version_name)=5 " +
          "group by App_version_name,Date_code"


        /*insert sql*/
        val sqlInsert = "insert into medusa_gray_testing_crash_info_each_day(day," +
          "buildDate,product_code,app_version_name,crashType,total_number,total_user) VALUES(?,?,?,?,?,?,?)"


        val versionProductOutofmemoryRdd = sqlContext.sql(versionProductOutofmemorySql).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()

        val versionProductNullpointerRdd = sqlContext.sql(versionProductNullpointerSql).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()

        val versionProductAllRdd = sqlContext.sql(versionProductAllSql).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()


        versionProductOutofmemoryRdd.foreach(r=>{
          util.insert(sqlInsert,day,r._2,r._3,r._1,"OOM",new JLong(r._4),new JLong(r._5))
        })
        versionProductNullpointerRdd.foreach(r=>{
          util.insert(sqlInsert,day,r._2,r._3,r._1,"NPE",new JLong(r._4),new JLong(r._5))
        })
        versionProductAllRdd.foreach(r=>{
          util.insert(sqlInsert,day,r._2,r._3,r._1,"All",new JLong(r._4),new JLong(r._5))
        })

      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}
