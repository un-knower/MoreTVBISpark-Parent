package com.moretv.bi.report.medusa.medusaGrayTestingStatistic

/**
 * Created by Administrator on 2016/3/28.
 */

import java.lang.{Long => JLong}

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json.JSONObject
import com.moretv.bi.medusa.util.ParquetDataStyle.ALL_CRASH_INFO

object CrashTrendsByVersionProductCrashKey extends SparkSetting{
  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

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
        val filterOutOfMemoryBuildDateRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log
          ._7,log._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._7=="20160518").filter(_._5.contains("java.lang" +
          ".OutOfMemoryError")).cache()
        val filterOutOfMemoryUnBuildRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log
          ._7,log._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._7!="20160518").filter(_._5.contains("java.lang." +
          "OutOfMemoryError")).cache()
        val filterOutofMemoryRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log
          ._7,log._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._5.contains("java.lang." +
          "OutOfMemoryError")).cache()
        val filterNullPointerBuildDateRdd =logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log
          ._7,log._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._7=="20160518").filter(_._5.contains("java.lang.NullPointerException")).
          cache()
        val filterNullPointerUnBuildDateRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log
          ._7,log._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._7!="20160518").filter(_._5.contains("java" +
          ".lang.NullPointerException")).cache()
        val filterNullPointerRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log
          ._7,log._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._5.contains("java" +
          ".lang.NullPointerException")).cache()
        val filterAllBuildDateRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log._7,log
          ._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._7=="20160518").cache()
        val filterAllunBuildDateRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log._7,log
          ._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._7!="20160518").cache()
        val filterAllRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log._7,log
          ._8)).filter(data => !DevMacUtils.macFilter(data._2)).cache()

        /**
         * Transform the RDD to DataFrame
         */
        val DF0 = filterOutOfMemoryBuildDateRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF0.registerTempTable("crashInfo0")
        val DF1 = filterOutOfMemoryUnBuildRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF1.registerTempTable("crashInfo1")
        val DF2 = filterOutofMemoryRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF2.registerTempTable("crashInfo2")
        val DF3 = filterNullPointerBuildDateRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF3.registerTempTable("crashInfo3")
        val DF4 = filterNullPointerUnBuildDateRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF4.registerTempTable("crashInfo4")
        val DF5 = filterNullPointerRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF5.registerTempTable("crashInfo5")
        val DF6 = filterAllBuildDateRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF6.registerTempTable("crashInfo6")
        val DF7 = filterAllunBuildDateRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF7.registerTempTable("crashInfo7")
        val DFAll = filterAllRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,data._6,data._7,data._8))
          .toDF()
        DFAll.registerTempTable("crashInfoAll")


        /**
         * 统计不同的crashType和buildDate情况
         */
        /*query sql*/
        val version_product_outofmemory_builddate_sql = "select Mac,App_version_name,Product_code from crashInfo0 where " +
          "App_version_name is not null and App_version_name != '' and length(App_version_name)=5"
        val version_product_outofmemory_unbuilddate_sql = "select Mac,App_version_name,Product_code from crashInfo1 where " +
          "App_version_name is not null and App_version_name != '' and length(App_version_name)=5"
        val version_product_outofmemory_sql = "select Mac,App_version_name,Product_code from crashInfo2 where " +
          "App_version_name is not null and App_version_name != '' and length(App_version_name)=5"
        val version_product_nullpointer_builddate_sql = "select Mac,App_version_name,Product_code from crashInfo3 where " +
          "App_version_name is not null and App_version_name != '' and length(App_version_name)=5"
        val version_product_nullpointer_unbuilddate_sql = "select Mac,App_version_name,Product_code from crashInfo4 where " +
          "App_version_name is not null and App_version_name != '' and length(App_version_name)=5"
        val version_product_nullpointer_sql = "select Mac,App_version_name,Product_code from crashInfo5 where " +
          "App_version_name is not null and App_version_name != '' and length(App_version_name)=5"
        val version_product_all_builddate_sql = "select Mac,App_version_name,Product_code from crashInfo6 where " +
          "App_version_name is not null and App_version_name != '' and length(App_version_name)=5"
        val version_product_all_unbuilddate_sql = "select Mac,App_version_name,Product_code from crashInfo7 where " +
          "App_version_name is not null and App_version_name != '' and length(App_version_name)=5"
        val version_product_all_sql = "select Mac,App_version_name,Product_code from crashInfoAll where " +
          "App_version_name is not null and App_version_name != '' and length(App_version_name)=5"

        /*insert sql*/
        val sql_num = "INSERT INTO medusa_crash_product_version_builddate_special_crash_key_num(day,buildDate," +
          "product_code,app_version_name,crashType,total_number) VALUES(?,?,?,?,?,?)"
        val sql_user ="INSERT INTO medusa_crash_product_version_builddate_special_crash_key_user(day,buildDate," +
          "product_code,app_version_name,crashType,total_user) VALUES(?,?,?,?,?,?)"

        /*calculate the data*/
        /*==================================OOM: Out of Memory===============================*/
        // buildDate: 20160518
        val total_outofmemory_builddate_num = sqlContext.sql(version_product_outofmemory_builddate_sql).count()
        val total_outofmemory_builddate_user = sqlContext.sql(version_product_outofmemory_builddate_sql).distinct.count()
        util.insert(sql_num,day,"20160518","All","All","java.lang.OutOfMemoryError",new JLong(total_outofmemory_builddate_num))
        util.insert(sql_user,day,"20160518","All","All","java.lang.OutOfMemoryError",new JLong(total_outofmemory_builddate_user))
        // 分版本，分型号
        val OOM_builddate_product_num_array = sqlContext.sql(version_product_outofmemory_builddate_sql)
          .groupBy("Product_code").count().collect()
        val OOM_builddate_date_num_array = sqlContext.sql(version_product_outofmemory_builddate_sql)
          .groupBy("App_version_name").count().collect()
        val OOM_builddate_product_date_num_array = sqlContext.sql(version_product_outofmemory_builddate_sql)
          .groupBy("Product_code","App_version_name").count().collect()
        // the number of user
        val OOM_builddate_product_user_num_array = sqlContext.sql(version_product_outofmemory_builddate_sql)
          .distinct().groupBy("Product_code").count().collect()
        val OOM_builddate_date_user_num_array = sqlContext.sql(version_product_outofmemory_builddate_sql)
          .distinct().groupBy("App_version_name").count().collect()
        val OOM_builddate_product_date_user_num_array = sqlContext.sql(version_product_outofmemory_builddate_sql)
          .distinct().groupBy("Product_code","App_version_name").count().collect()
        //插入
        OOM_builddate_product_num_array.foreach(row=>{
          println("-----------The total number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"20160518",row.getString(0),"All","java.lang.OutOfMemoryError",new JLong(row.getLong(1)))
        })
        OOM_builddate_date_num_array.foreach(row=>{
          println("-----------The total number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"20160518","All",row.getString(0),"java.lang.OutOfMemoryError",new JLong(row.getLong(1)))
        })
        OOM_builddate_product_date_num_array.foreach(row=>{
          println("-----------The total number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_num,day,"20160518",row.getString(0),row.getString(1),"java.lang.OutOfMemoryError",new JLong(row.getLong(2)))
        })

        OOM_builddate_product_user_num_array.foreach(row=>{
          println("-----------The total user number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"20160518",row.getString(0),"All","java.lang.OutOfMemoryError",new JLong(row.getLong(1)))
        })
        OOM_builddate_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"20160518","All",row.getString(0),"java.lang.OutOfMemoryError",new JLong(row.getLong(1)))
        })
        OOM_builddate_product_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_user,day,"20160518",row.getString(0),row.getString(1),"java.lang.OutOfMemoryError",new JLong(row.getLong(2)))
        })
        filterOutOfMemoryBuildDateRdd.unpersist()

        // buildDate: 20160518之前
        val total_outofmemory_unbuilddate_num = sqlContext.sql(version_product_outofmemory_unbuilddate_sql).count()
        val total_outofmemory_unbuilddate_user = sqlContext.sql(version_product_outofmemory_unbuilddate_sql).distinct.count()
        util.insert(sql_num,day,"before 20160518","All","All","java.lang.OutOfMemoryError",new JLong
        (total_outofmemory_unbuilddate_num))
        util.insert(sql_user,day,"before 20160518","All","All","java.lang.OutOfMemoryError",new JLong
        (total_outofmemory_unbuilddate_user))
        // 分版本，分型号
        val OOM_unbuilddate_product_num_array = sqlContext.sql(version_product_outofmemory_unbuilddate_sql)
          .groupBy("Product_code").count().collect()
        val OOM_unbuilddate_date_num_array = sqlContext.sql(version_product_outofmemory_unbuilddate_sql)
          .groupBy("App_version_name").count().collect()
        val OOM_unbuilddate_product_date_num_array = sqlContext.sql(version_product_outofmemory_unbuilddate_sql)
          .groupBy("Product_code","App_version_name").count().collect()
        // the number of user
        val OOM_unbuilddate_product_user_num_array = sqlContext.sql(version_product_outofmemory_unbuilddate_sql)
          .distinct().groupBy("Product_code").count().collect()
        val OOM_unbuilddate_date_user_num_array = sqlContext.sql(version_product_outofmemory_unbuilddate_sql)
          .distinct().groupBy("App_version_name").count().collect()
        val OOM_unbuilddate_product_date_user_num_array = sqlContext.sql(version_product_outofmemory_unbuilddate_sql)
          .distinct().groupBy("Product_code","App_version_name").count().collect()
        //插入
        OOM_unbuilddate_product_num_array.foreach(row=>{
          println("-----------The total number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"before 20160518",row.getString(0),"All","java.lang.OutOfMemoryError",new JLong(row.getLong(1)))
        })
        OOM_unbuilddate_date_num_array.foreach(row=>{
          println("-----------The total number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"before 20160518","All",row.getString(0),"java.lang.OutOfMemoryError",new JLong(row.getLong(1)))
        })
        OOM_unbuilddate_product_date_num_array.foreach(row=>{
          println("-----------The total number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_num,day,"before 20160518",row.getString(0),row.getString(1),"java.lang.OutOfMemoryError",new JLong(row.getLong(2)))
        })

        OOM_unbuilddate_product_user_num_array.foreach(row=>{
          println("-----------The total user number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"before 20160518",row.getString(0),"All","java.lang.OutOfMemoryError",new JLong(row
            .getLong(1)))
        })
        OOM_unbuilddate_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"before 20160518","All",row.getString(0),"java.lang.OutOfMemoryError",new JLong(row
            .getLong(1)))
        })
        OOM_unbuilddate_product_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_user,day,"before 20160518",row.getString(0),row.getString(1),"java.lang.OutOfMemoryError",new JLong(row.getLong(2)))
        })
        filterOutOfMemoryUnBuildRdd.unpersist()

        // 所有的outofmemory
        val total_outofmemory_num = sqlContext.sql(version_product_outofmemory_sql).count()
        val total_outofmemory_user = sqlContext.sql(version_product_outofmemory_sql).distinct.count()
        util.insert(sql_num,day,"All","All","All","java.lang.OutOfMemoryError",new JLong
        (total_outofmemory_num))
        util.insert(sql_user,day,"All","All","All","java.lang.OutOfMemoryError",new JLong
        (total_outofmemory_user))
        // 分版本，分型号
        val OOM_product_num_array = sqlContext.sql(version_product_outofmemory_sql)
          .groupBy("Product_code").count().collect()
        val OOM_date_num_array = sqlContext.sql(version_product_outofmemory_sql)
          .groupBy("App_version_name").count().collect()
        val OOM_product_date_num_array = sqlContext.sql(version_product_outofmemory_sql)
          .groupBy("Product_code","App_version_name").count().collect()
        // the number of user
        val OOM_product_user_num_array = sqlContext.sql(version_product_outofmemory_sql)
          .distinct().groupBy("Product_code").count().collect()
        val OOM_date_user_num_array = sqlContext.sql(version_product_outofmemory_sql)
          .distinct().groupBy("App_version_name").count().collect()
        val OOM_product_date_user_num_array = sqlContext.sql(version_product_outofmemory_sql)
          .distinct().groupBy("Product_code","App_version_name").count().collect()
        //插入
        OOM_product_num_array.foreach(row=>{
          println("-----------The total number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"All",row.getString(0),"All","java.lang.OutOfMemoryError",new JLong(row.getLong(1)))
        })
        OOM_date_num_array.foreach(row=>{
          println("-----------The total number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"All","All",row.getString(0),"java.lang.OutOfMemoryError",new JLong(row.getLong(1)))
        })
        OOM_product_date_num_array.foreach(row=>{
          println("-----------The total number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_num,day,"All",row.getString(0),row.getString(1),"java.lang.OutOfMemoryError",new JLong(row.getLong
            (2)))
        })

        OOM_product_user_num_array.foreach(row=>{
          println("-----------The total user number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"All",row.getString(0),"All","java.lang.OutOfMemoryError",new JLong(row.getLong(1)))
        })
        OOM_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"All","All",row.getString(0),"java.lang.OutOfMemoryError",new JLong(row.getLong(1)))
        })
        OOM_product_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_user,day,"All",row.getString(0),row.getString(1),"java.lang.OutOfMemoryError",new JLong(row
            .getLong(2)))
        })
        filterOutofMemoryRdd.unpersist()


        /*============================NPE: Null Pointer Exception==================*/

        // buildDate: 20160518
        val total_nullpointer_builddate_num = sqlContext.sql(version_product_nullpointer_builddate_sql).count()
        val total_nullpointer_builddate_user = sqlContext.sql(version_product_nullpointer_builddate_sql).distinct.count()
        util.insert(sql_num,day,"20160518","All","All","java.lang.NullPointerException",new JLong(total_nullpointer_builddate_num))
        util.insert(sql_user,day,"20160518","All","All","java.lang.NullPointerException",new JLong(total_nullpointer_builddate_user))
        // 分版本，分型号
        val NPE_builddate_product_num_array = sqlContext.sql(version_product_nullpointer_builddate_sql)
          .groupBy("Product_code").count().collect()
        val NPE_builddate_date_num_array = sqlContext.sql(version_product_nullpointer_builddate_sql)
          .groupBy("App_version_name").count().collect()
        val NPE_builddate_product_date_num_array = sqlContext.sql(version_product_nullpointer_builddate_sql)
          .groupBy("Product_code","App_version_name").count().collect()
        // the number of user
        val NPE_builddate_product_user_num_array = sqlContext.sql(version_product_nullpointer_builddate_sql)
          .distinct().groupBy("Product_code").count().collect()
        val NPE_builddate_date_user_num_array = sqlContext.sql(version_product_nullpointer_builddate_sql)
          .distinct().groupBy("App_version_name").count().collect()
        val NPE_builddate_product_date_user_num_array = sqlContext.sql(version_product_nullpointer_builddate_sql)
          .distinct().groupBy("Product_code","App_version_name").count().collect()

        //插入
        NPE_builddate_product_num_array.foreach(row=>{
          println("-----------The total number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"20160518",row.getString(0),"All","java.lang.NullPointerException",new JLong(row.getLong
            (1)))
        })
        NPE_builddate_date_num_array.foreach(row=>{
          println("-----------The total number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"20160518","All",row.getString(0),"java.lang.NullPointerException",new JLong(row.getLong
            (1)))
        })
        NPE_builddate_product_date_num_array.foreach(row=>{
          println("-----------The total number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_num,day,"20160518",row.getString(0),row.getString(1),"java.lang.NullPointerException",new JLong
          (row.getLong(2)))
        })

        NPE_builddate_product_user_num_array.foreach(row=>{
          println("-----------The total user number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"20160518",row.getString(0),"All","java.lang.NullPointerException",new JLong(row
            .getLong(1)))
        })
        NPE_builddate_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"20160518","All",row.getString(0),"java.lang.NullPointerException",new JLong(row
            .getLong(1)))
        })
        NPE_builddate_product_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_user,day,"20160518",row.getString(0),row.getString(1),"java.lang.NullPointerException",new JLong
          (row.getLong(2)))
        })
        filterNullPointerBuildDateRdd.unpersist()


        // buildDate: 20160518之前
        val total_nullpointer_unbuilddate_num = sqlContext.sql(version_product_nullpointer_unbuilddate_sql).count()
        val total_nullpointer_unbuilddate_user = sqlContext.sql(version_product_nullpointer_unbuilddate_sql).distinct.count()
        util.insert(sql_num,day,"before 20160518","All","All","java.lang.NullPointerException",new JLong
        (total_nullpointer_unbuilddate_num))
        util.insert(sql_user,day,"before 20160518","All","All","java.lang.NullPointerException",new JLong
        (total_nullpointer_unbuilddate_user))
        // 分版本，分型号
        val NPE_unbuilddate_product_num_array = sqlContext.sql(version_product_nullpointer_unbuilddate_sql)
          .groupBy("Product_code").count().collect()
        val NPE_unbuilddate_date_num_array = sqlContext.sql(version_product_nullpointer_unbuilddate_sql)
          .groupBy("App_version_name").count().collect()
        val NPE_unbuilddate_product_date_num_array = sqlContext.sql(version_product_nullpointer_unbuilddate_sql)
          .groupBy("Product_code","App_version_name").count().collect()
        // the number of user
        val NPE_unbuilddate_product_user_num_array = sqlContext.sql(version_product_nullpointer_unbuilddate_sql)
          .distinct().groupBy("Product_code").count().collect()
        val NPE_unbuilddate_date_user_num_array = sqlContext.sql(version_product_nullpointer_unbuilddate_sql)
          .distinct().groupBy("App_version_name").count().collect()
        val NPE_unbuilddate_product_date_user_num_array = sqlContext.sql(version_product_nullpointer_unbuilddate_sql)
          .distinct().groupBy("Product_code","App_version_name").count().collect()

        //插入
        NPE_unbuilddate_product_num_array.foreach(row=>{
          println("-----------The total number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"before 20160518",row.getString(0),"All","java.lang.NullPointerException",new JLong(row
            .getLong(1)))
        })
        NPE_unbuilddate_date_num_array.foreach(row=>{
          println("-----------The total number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"before 20160518","All",row.getString(0),"java.lang.NullPointerException",new JLong(row
            .getLong(1)))
        })
        NPE_unbuilddate_product_date_num_array.foreach(row=>{
          println("-----------The total number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_num,day,"before 20160518",row.getString(0),row.getString(1),"java.lang.NullPointerException",new
              JLong(row.getLong(2)))
        })

        NPE_unbuilddate_product_user_num_array.foreach(row=>{
          println("-----------The total user number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"before 20160518",row.getString(0),"All","java.lang.NullPointerException",new JLong(row
            .getLong(1)))
        })
        NPE_unbuilddate_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"before 20160518","All",row.getString(0),"java.lang.NullPointerException",new JLong(row
            .getLong(1)))
        })
        NPE_unbuilddate_product_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_user,day,"before 20160518",row.getString(0),row.getString(1),"java.lang.NullPointerException",
            new JLong(row.getLong(2)))
        })
        filterNullPointerUnBuildDateRdd.unpersist()


        // 所有的null pointer exception
        val total_nullpointer_num = sqlContext.sql(version_product_nullpointer_sql).count()
        val total_nullpointer_user = sqlContext.sql(version_product_nullpointer_sql).distinct.count()
        util.insert(sql_num,day,"All","All","All","java.lang.NullPointerException",new JLong
        (total_nullpointer_num))
        util.insert(sql_user,day,"All","All","All","java.lang.NullPointerException",new JLong
        (total_nullpointer_user))
        // 分版本，分型号
        val NPE_product_num_array = sqlContext.sql(version_product_nullpointer_sql)
          .groupBy("Product_code").count().collect()
        val NPE_date_num_array = sqlContext.sql(version_product_nullpointer_sql)
          .groupBy("App_version_name").count().collect()
        val NPE_product_date_num_array = sqlContext.sql(version_product_nullpointer_sql)
          .groupBy("Product_code","App_version_name").count().collect()
        // the number of user
        val NPE_product_user_num_array = sqlContext.sql(version_product_nullpointer_sql)
          .distinct().groupBy("Product_code").count().collect()
        val NPE_date_user_num_array = sqlContext.sql(version_product_nullpointer_sql)
          .distinct().groupBy("App_version_name").count().collect()
        val NPE_product_date_user_num_array = sqlContext.sql(version_product_nullpointer_sql)
          .distinct().groupBy("Product_code","App_version_name").count().collect()

        //插入
        NPE_product_num_array.foreach(row=>{
          println("-----------The total number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"All",row.getString(0),"All","java.lang.NullPointerException",new JLong(row.getLong(1)))
        })
        NPE_date_num_array.foreach(row=>{
          println("-----------The total number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"All","All",row.getString(0),"java.lang.NullPointerException",new JLong(row.getLong(1)))
        })
        NPE_product_date_num_array.foreach(row=>{
          println("-----------The total number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_num,day,"All",row.getString(0),row.getString(1),"java.lang.NullPointerException",new JLong(row
            .getLong(2)))
        })

        NPE_product_user_num_array.foreach(row=>{
          println("-----------The total user number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"All",row.getString(0),"All","java.lang.NullPointerException",new JLong(row.getLong(1)))
        })
        NPE_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"All","All",row.getString(0),"java.lang.NullPointerException",new JLong(row.getLong(1)))
        })
        NPE_product_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_user,day,"All",row.getString(0),row.getString(1),"java.lang.NullPointerException",new JLong(row
            .getLong(2)))
        })
        filterNullPointerRdd.unpersist()

        /*============================All crash==================*/

        // buildDate: 20160518
        val total_builddate_num = sqlContext.sql(version_product_all_builddate_sql).count()
        val total_builddate_user = sqlContext.sql(version_product_all_builddate_sql).distinct.count()
        util.insert(sql_num,day,"20160518","All","All","All",new JLong(total_builddate_num))
        util.insert(sql_user,day,"20160518","All","All","All",new JLong(total_builddate_user))
        // 分版本，分型号
        val all_builddate_product_num_array = sqlContext.sql(version_product_all_builddate_sql)
          .groupBy("Product_code").count().collect()
        val all_builddate_date_num_array = sqlContext.sql(version_product_all_builddate_sql)
          .groupBy("App_version_name").count().collect()
        val all_builddate_product_date_num_array = sqlContext.sql(version_product_all_builddate_sql)
          .groupBy("Product_code","App_version_name").count().collect()
        // the number of user
        val all_builddate_product_user_num_array = sqlContext.sql(version_product_all_builddate_sql)
          .distinct().groupBy("Product_code").count().collect()
        val all_builddate_date_user_num_array = sqlContext.sql(version_product_all_builddate_sql)
          .distinct().groupBy("App_version_name").count().collect()
        val all_builddate_product_date_user_num_array = sqlContext.sql(version_product_all_builddate_sql)
          .distinct().groupBy("Product_code","App_version_name").count().collect()

        //插入
        all_builddate_product_num_array.foreach(row=>{
          println("-----------The total number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"20160518",row.getString(0),"All","All",new JLong(row.getLong(1)))
        })
        all_builddate_date_num_array.foreach(row=>{
          println("-----------The total number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"20160518","All",row.getString(0),"All",new JLong(row.getLong(1)))
        })
        all_builddate_product_date_num_array.foreach(row=>{
          println("-----------The total number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_num,day,"20160518",row.getString(0),row.getString(1),"All",new JLong(row.getLong(2)))
        })

        all_builddate_product_user_num_array.foreach(row=>{
          println("-----------The total user number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"20160518",row.getString(0),"All","All",new JLong(row.getLong(1)))
        })
        all_builddate_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"20160518","All",row.getString(0),"All",new JLong(row.getLong(1)))
        })
        all_builddate_product_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_user,day,"20160518",row.getString(0),row.getString(1),"All",new JLong(row.getLong(2)))
        })
        filterAllBuildDateRdd.unpersist()


        // buildDate: 20160518之前
        val total_unbuilddate_num = sqlContext.sql(version_product_all_unbuilddate_sql).count()
        val total_unbuilddate_user = sqlContext.sql(version_product_all_unbuilddate_sql).distinct.count()
        util.insert(sql_num,day,"before 20160518","All","All","java.lang.NullPointerException",new JLong
        (total_unbuilddate_num))
        util.insert(sql_user,day,"before 20160518","All","All","java.lang.NullPointerException",new JLong
        (total_unbuilddate_user))
        // 分版本，分型号
        val all_unbuilddate_product_num_array = sqlContext.sql(version_product_all_unbuilddate_sql)
          .groupBy("Product_code").count().collect()
        val all_unbuilddate_date_num_array = sqlContext.sql(version_product_all_unbuilddate_sql)
          .groupBy("App_version_name").count().collect()
        val all_unbuilddate_product_date_num_array = sqlContext.sql(version_product_all_unbuilddate_sql)
          .groupBy("Product_code","App_version_name").count().collect()
        // the number of user
        val all_unbuilddate_product_user_num_array = sqlContext.sql(version_product_all_unbuilddate_sql)
          .distinct().groupBy("Product_code").count().collect()
        val all_unbuilddate_date_user_num_array = sqlContext.sql(version_product_all_unbuilddate_sql)
          .distinct().groupBy("App_version_name").count().collect()
        val all_unbuilddate_product_date_user_num_array = sqlContext.sql(version_product_all_unbuilddate_sql)
          .distinct().groupBy("Product_code","App_version_name").count().collect()

        //插入
        all_unbuilddate_product_num_array.foreach(row=>{
          println("-----------The total number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"before 20160518",row.getString(0),"All","All",new JLong(row.getLong(1)))
        })
        all_unbuilddate_date_num_array.foreach(row=>{
          println("-----------The total number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"before 20160518","All",row.getString(0),"All",new JLong(row.getLong(1)))
        })
        all_unbuilddate_product_date_num_array.foreach(row=>{
          println("-----------The total number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_num,day,"before 20160518",row.getString(0),row.getString(1),"All",new JLong(row.getLong(2)))
        })

        all_unbuilddate_product_user_num_array.foreach(row=>{
          println("-----------The total user number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"before 20160518",row.getString(0),"All","All",new JLong(row.getLong(1)))
        })
        all_unbuilddate_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"before 20160518","All",row.getString(0),"All",new JLong(row.getLong(1)))
        })
        all_unbuilddate_product_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_user,day,"before 20160518",row.getString(0),row.getString(1),"All",new JLong(row.getLong(2)))
        })
        filterAllunBuildDateRdd.unpersist()

        // 所有的outofmemory
        val total_num = sqlContext.sql(version_product_all_sql).count()
        val total_user = sqlContext.sql(version_product_all_sql).distinct.count()
        util.insert(sql_num,day,"All","All","All","java.lang.NullPointerException",new JLong
        (total_num))
        util.insert(sql_user,day,"All","All","All","java.lang.NullPointerException",new JLong
        (total_user))
        // 分版本，分型号
        val all_product_num_array = sqlContext.sql(version_product_all_sql)
          .groupBy("Product_code").count().collect()
        val all_date_num_array = sqlContext.sql(version_product_all_sql)
          .groupBy("App_version_name").count().collect()
        val all_product_date_num_array = sqlContext.sql(version_product_all_sql)
          .groupBy("Product_code","App_version_name").count().collect()
        // the number of user
        val all_product_user_num_array = sqlContext.sql(version_product_all_sql)
          .distinct().groupBy("Product_code").count().collect()
        val all_date_user_num_array = sqlContext.sql(version_product_all_sql)
          .distinct().groupBy("App_version_name").count().collect()
        val all_product_date_user_num_array = sqlContext.sql(version_product_all_sql)
          .distinct().groupBy("Product_code","App_version_name").count().collect()

        //插入
        all_product_num_array.foreach(row=>{
          println("-----------The total number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"All",row.getString(0),"All","All",new JLong(row.getLong(1)))
        })
        all_date_num_array.foreach(row=>{
          println("-----------The total number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"All","All",row.getString(0),"All",new JLong(row.getLong(1)))
        })
        all_product_date_num_array.foreach(row=>{
          println("-----------The total number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_num,day,"All",row.getString(0),row.getString(1),"All",new JLong(row.getLong(2)))
        })

        all_product_user_num_array.foreach(row=>{
          println("-----------The total user number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"All",row.getString(0),"All","All",new JLong(row.getLong(1)))
        })
        all_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"All","All",row.getString(0),"All",new JLong(row.getLong(1)))
        })
        all_product_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_user,day,"All",row.getString(0),row.getString(1),"All",new JLong(row.getLong(2)))
        })
        filterAllRdd.unpersist()

      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}
