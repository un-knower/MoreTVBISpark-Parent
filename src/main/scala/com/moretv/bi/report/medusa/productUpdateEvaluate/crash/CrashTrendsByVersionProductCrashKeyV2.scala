package com.moretv.bi.report.medusa.productUpdateEvaluate.crash

/**
 * Created by Administrator on 2016/3/28.
 */

import java.lang.{Long => JLong}

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.report.medusa.CrashLog.EachUserEachCrashAppearInfo.MEDUSA
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object CrashTrendsByVersionProductCrashKeyV2 extends SparkSetting{
  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate,-1)

        // 过滤掉stack_trace没有值/空的情形
        val logRdd = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.CRASH_LOG,inputDate).
          select("MAC","APP_VERSION_NAME","APP_VERSION_CODE","CRASH_KEY","STACK_TRACE","DATE_CODE","PRODUCT_CODE").
          filter("APP_VERSION_CODE is not null and DATE_CODE is not null").
          map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getString(3),e.getString(4),
            e.getLong(5),e.getString(6))).filter(e=>{e._2!=null}).map(e=>(e._1,
          e._2.replace(":",""),e._3.toString, e._4,e._5,e._6.toString,e._7)).filter(e=>{e._5!=null && e._5!=""  && {if(e._6!=null) e._6.length<=20 else true}})

        // 内存溢出的crash
        logRdd.map(log => (log._1,log._2,log._3,log._4,log._5,log._6,log
          ._7,DigestUtils.md5Hex(log._4),DigestUtils.md5Hex(log._5))).filter(data => !DevMacUtils.macFilter(data._1)).filter(_._4.contains("java.lang." +
          "OutOfMemoryError")).toDF("Mac","App_version_name","App_version_code",
            "Crash_key","Stack_trace","Date_code","Product_code","Crash_key_md5",
            "Stack_trace_md5").registerTempTable("log_data_oom")
        sqlContext.sql(
          """
            |select Mac,App_version_name,App_version_code,Date_code,Product_code,Crash_key_md5,
            |Stack_trace_md5,count(Mac) as num
            |from log_data_oom
            |group by Mac,App_version_name,App_version_code,Date_code,Product_code,Crash_key_md5,
            |Stack_trace_md5
          """.stripMargin).toDF("Mac","App_version_name","App_version_code","Date_code","Product_code",
            "Crash_key_md5","Stack_trace_md5","num").filter("num<=1000").registerTempTable("crashInfo2")

        // 空指针的crash
        logRdd.map(log => (log._1,log._2,log._3,log._4,log._5,log._6,log
          ._7,DigestUtils.md5Hex(log._4),DigestUtils.md5Hex(log._5))).filter(data => !DevMacUtils.macFilter(data._1)).filter(_._4.contains("java" +
          ".lang.NullPointerException")).toDF("Mac","App_version_name","App_version_code",
            "Crash_key","Stack_trace","Date_code","Product_code","Crash_key_md5",
            "Stack_trace_md5").registerTempTable("log_data_nlp")
        sqlContext.sql(
          """
            |select Mac,App_version_name,App_version_code,Date_code,Product_code,Crash_key_md5,
            |Stack_trace_md5,count(Mac) as num
            |from log_data_nlp
            |group by Mac,App_version_name,App_version_code,Date_code,Product_code,Crash_key_md5,
            |Stack_trace_md5
          """.stripMargin).toDF("Mac","App_version_name","App_version_code","Date_code","Product_code",
            "Crash_key_md5","Stack_trace_md5","num").filter("num<=1000").registerTempTable("crashInfo5")
        // 所有的crash
        logRdd.map(log => (log._1,log._2,log._3,log._4,log._5,log._6,log._7,DigestUtils.md5Hex(log._4),DigestUtils.md5Hex(log._5))).
          filter(data => !DevMacUtils.macFilter(data._2)).toDF("Mac","App_version_name","App_version_code",
            "Crash_key","Stack_trace","Date_code","Product_code","Crash_key_md5",
            "Stack_trace_md5").registerTempTable("log_data")

        sqlContext.sql(
          """
            |select Mac,App_version_name,App_version_code,Date_code,Product_code,Crash_key_md5,
            |Stack_trace_md5,count(Mac) as num
            |from log_data
            |group by Mac,App_version_name,App_version_code,Date_code,Product_code,Crash_key_md5,
            |Stack_trace_md5
          """.stripMargin).toDF("Mac","App_version_name","App_version_code","Date_code","Product_code",
            "Crash_key_md5","Stack_trace_md5","num").filter("num<=1000").registerTempTable("crashInfoAll")

        if(p.deleteOld){
          val deleteSql = "delete from medusa_product_update_crash_ratio_info_each_day where day = ?"
          util.delete(deleteSql,day)
        }

        /*insert sql*/
        val sqlInsert = "insert into medusa_product_update_crash_ratio_info_each_day(day,buildDate,product_code," +
          "app_version_name," +
          "crashType,total_number,total_user) VALUES(?,?,?,?,?,?,?)"
        /**
         * 统计不同的crashType和buildDate情况S
         */
        /*query sql*/

        // 统计各个终端型号、apkVersion、BuildDate的OOM crash
        val sql1 = "select App_version_name,Date_code,Product_code,sum(num),count(distinct " +
          "Mac) from crashInfo2 where App_version_name is not null and App_version_name != '' and length(App_version_name)" +
          "=5 group by App_version_name,Date_code,Product_code"
        val rdd1 = sqlContext.sql(sql1).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
        rdd1.foreach(r=>{
          try{
            util.insert(sqlInsert,day,r._2,r._3,r._1,"OOM",new JLong(r._4),new JLong(r._5))
          }catch {
            case e:Exception =>
          }
        })
        // 统计各个apkVersion、BuildDate的OOM crash
        val sql2 = "select App_version_name,Date_code,'All' as Product_code,sum(num),count(distinct " +
          "Mac) from crashInfo2 where App_version_name is not null and App_version_name != '' and length(App_version_name)" +
          "=5 group by App_version_name,Date_code"
        val rdd2 = sqlContext.sql(sql2).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
        rdd2.foreach(r=>{
          try {
            util.insert(sqlInsert,day,r._2,r._3,r._1,"OOM",new JLong(r._4),new JLong(r._5))
          }catch {
            case e:Exception =>
          }
        })
        // 统计各个终端型号、apkVersion、BuildDate的NPE crash
        val sql3 = "select App_version_name,Date_code,Product_code,sum(num),count(distinct Mac) " +
          "from crashInfo5 where App_version_name is not null and App_version_name != '' and length(App_version_name)=5 " +
          "group by App_version_name,Date_code,Product_code"
        val rdd3 = sqlContext.sql(sql3).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
        rdd3.foreach(r=>{
          try {
            util.insert(sqlInsert,day,r._2,r._3,r._1,"NPE",new JLong(r._4),new JLong(r._5))
          }catch {
            case e:Exception =>
          }
        })
        // 统计各个apkVersion、BuildDate的NPE crash
        val sql4 = "select App_version_name,Date_code,'All' as Product_code,sum(num),count(distinct " +
          "Mac) from crashInfo5 where App_version_name is not null and App_version_name != '' and length(App_version_name)=5 " +
          "group by App_version_name,Date_code"
        val rdd4 = sqlContext.sql(sql4).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
        rdd4.foreach(r=>{
          try {
            util.insert(sqlInsert,day,r._2,r._3,r._1,"NPE",new JLong(r._4),new JLong(r._5))
          }catch {
            case e:Exception =>
          }
        })
        // 统计各个终端型号、apkVersion、BuildDate的所有crash
        val sql5 = "select App_version_name,Date_code,Product_code,sum(num),count(distinct Mac) " +
          "from crashInfoAll where App_version_name is not null and App_version_name != '' and length(App_version_name)=5 " +
          "group by App_version_name,Date_code,Product_code "
        val rdd5 = sqlContext.sql(sql5).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
        rdd5.foreach(r=>{
          try {
            util.insert(sqlInsert,day,r._2,r._3,r._1,"All",new JLong(r._4),new JLong(r._5))
          }catch {
            case e:Exception=>
          }
        })
        // 统计各个apkVersion、BuildDate的所有 crash
        val sql6 = "select App_version_name,Date_code,'All' as Product_code,sum(num),count(distinct " +
          "Mac) from crashInfoAll where App_version_name is not null and App_version_name != '' and length(App_version_name)=5 group by App_version_name,Date_code"
        val rdd6 = sqlContext.sql(sql6).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
        rdd6.foreach(r=>{
          try {
            util.insert(sqlInsert,day,r._2,r._3,r._1,"All",new JLong(r._4),new JLong(r._5))
          }catch {
            case e:Exception =>
          }
        })
        // 统计所有版本所有终端的所有crash
        val sql7 = "select 'All','All','All' as Product_code,sum(num),count(distinct " +
          "Mac) from crashInfoAll where App_version_name is not null and App_version_name != '' and length" +
          "(App_version_name)=5"
        val rdd7 = sqlContext.sql(sql7).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
        rdd7.foreach(r=>{
          try {
            util.insert(sqlInsert,day,r._2,r._3,r._1,"All",new JLong(r._4),new JLong(r._5))
          }catch {
            case e:Exception =>
          }
        })

        // 统计除了某个版本之外的所有情况
        if(p.apkVersion!=""){
          val apkVersion = p.apkVersion
          // 统计各个终端型号的OOM crash
          val sql1 = s"select '','', Product_code,sum(num),count(distinct Mac) from crashInfo2 where App_version_name is" +
            s" not null and App_version_name != '' and length(App_version_name)=5 and App_version_name !='$apkVersion' " +
            s"group by Product_code"
          val rdd1 = sqlContext.sql(sql1).map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
          rdd1.foreach(r=>{
            try {
              util.insert(sqlInsert,day,"All",r._3,"Non-".concat(apkVersion),"OOM",new JLong(r._4),new JLong(r._5))
            }catch {
              case e:Exception =>
            }
          })
          // 统计各个所有终端的OOM crash
          val sql2 = s"select '','','All' as Product_code,sum(num),count(distinct Mac) from crashInfo2 where " +
            s"App_version_name is not null and App_version_name != '' and length(App_version_name)=5 and " +
            s"App_version_name!='$apkVersion'"
          val rdd2 = sqlContext.sql(sql2).map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
          rdd2.foreach(r=>{
            try {
              util.insert(sqlInsert,day,"All",r._3,"Non-".concat(apkVersion),"OOM",new JLong(r._4),new JLong(r._5))

            }
            catch {
              case e:Exception =>
            }
          })
          // 统计各个终端型号、apkVersion、BuildDate的NPE crash
          val sql3 = s"select '','',Product_code,sum(num),count(distinct Mac) " +
            s"from crashInfo5 where App_version_name is not null and App_version_name != '' and length(App_version_name)=5" +
            s" and App_version_name!='$apkVersion' group by Product_code"
          val rdd3 = sqlContext.sql(sql3).map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
          rdd3.foreach(r=>{
            try {
              util.insert(sqlInsert,day,"All",r._3,"Non-".concat(apkVersion),"NPE",new JLong(r._4),new JLong(r._5))

            }catch {
              case e:Exception =>
            }
          })
          // 统计各个apkVersion、BuildDate的NPE crash
          val sql4 = s"select '','','All' as Product_code,sum(num),count(distinct Mac) from crashInfo5 where " +
            s"App_version_name is not null and App_version_name != '' and length(App_version_name)=5 and " +
            s"App_version_name!='$apkVersion'"
          val rdd4 = sqlContext.sql(sql4).map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
          rdd4.foreach(r=>{
            try {
              util.insert(sqlInsert,day,"All",r._3,"Non-".concat(apkVersion),"NPE",new JLong(r._4),new JLong(r._5))

            }catch {
              case e:Exception =>
            }
          })
          // 统计各个终端型号、apkVersion、BuildDate的所有crash
          val sql5 = s"select '','',Product_code,sum(num),count(distinct Mac) " +
            s"from crashInfoAll where App_version_name is not null and App_version_name != '' and length(App_version_name)" +
            s"=5 and App_version_name!='$apkVersion' group by Product_code "
          val rdd5 = sqlContext.sql(sql5).map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
          rdd5.foreach(r=>{
            try {
              util.insert(sqlInsert,day,"All",r._3,"Non-".concat(apkVersion),"All",new JLong(r._4),new JLong(r._5))

            }catch {
              case e:Exception =>
            }
          })
          // 统计的所有 crash
          val sql6 = "select '','','All' as Product_code,sum(num),count(distinct " +
            "Mac) from crashInfoAll where App_version_name is not null and App_version_name != '' and length" +
            s"(App_version_name)=5 and App_version_name!='$apkVersion'"
          val rdd6 = sqlContext.sql(sql6).map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
          rdd6.foreach(r=>{
            try {
              util.insert(sqlInsert,day,"All",r._3,"Non-".concat(apkVersion),"All",new JLong(r._4),new JLong(r._5))

            }catch {
              case e:Exception =>
            }
          })
        }

      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}
