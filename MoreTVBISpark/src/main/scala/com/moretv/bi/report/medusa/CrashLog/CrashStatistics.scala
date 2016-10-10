package com.moretv.bi.report.medusa.CrashLog

/**
 * Created by Administrator on 2016/3/28.
 */

import java.lang.{Long => JLong}

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json.JSONObject
import com.moretv.bi.medusa.util.ParquetDataStyle.ALL_CRASH_INFO

object CrashStatistics extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(CrashStatistics,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val s = sqlContext
        import s.implicits._
        val util = new DBOperationUtils("medusa")
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate)
        val logRdd = sc.textFile(s"/log/crash/metadata/${inputDate}_extraction.log").map(log=>{
          val json = new JSONObject(log)
          (json.optString("fileName"),json.optString("MAC"),json.optString("APP_VERSION_NAME"),json.optString("APP_VERSION_CODE"),
            json.optString("CRASH_KEY"),json.optString("STACK_TRACE"),json.optString("DATE_CODE"),json.optString("PRODUCT_CODE"))
        })

        val filterRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log._7,log._8))
          .filter(data => !DevMacUtils.macFilter(data._2)).cache()
        /**
         * Transform the RDD to DataFrame
         */
        val DF = filterRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,data._6,data._7,data._8)).toDF()
        DF.registerTempTable("crashInfo")


        /**
         * The statistic process for different needs
         *
         */
        println("------------------------Begin Statistic------------------------")

        //--------Statistic the number of crash/user based on DATE_CODE and PRODUCT_CODE
        val date_product_sql = "select Mac,Date_code,Product_code from crashInfo"
        val total_num = sqlContext.sql(date_product_sql).count()
        val total_user = sqlContext.sql(date_product_sql).distinct().count()
        println("Total number of crash is: "+total_num)
        println("Total number of user is: "+total_user)
        val sql_num = "INSERT INTO medusa_crash_product_date_code_num(day,product_code,date_code,total_number) VALUES(?,?," +
          "?,?)"
        val sql_user ="INSERT INTO medusa_crash_product_date_code_user(day,product_code,date_code,total_user) VALUES(?," +
          "?,?,?)"
        util.insert(sql_num,day,"All","All",new JLong(total_num))
        util.insert(sql_user,day,"All","All",new JLong(total_user))

        val product_num_array = sqlContext.sql(date_product_sql).groupBy("Product_code").count().collect()
        val date_num_array = sqlContext.sql(date_product_sql).groupBy("Date_code").count().collect()
        val product_date_num_array = sqlContext.sql(date_product_sql).groupBy("Product_code","Date_code").count().collect()
        // the number of user
        val product_user_num_array = sqlContext.sql(date_product_sql).distinct().groupBy("Product_code").count().collect()
        val date_user_num_array = sqlContext.sql(date_product_sql).distinct().groupBy("Date_code").count().collect()
        val product_date_user_num_array = sqlContext.sql(date_product_sql).distinct().groupBy("Product_code","Date_code")
          .count()
          .collect()

        product_num_array.foreach(row=>{
          println("-----------The total number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,row.getString(0),"All",new JLong(row.getLong(1)))
        })
        date_num_array.foreach(row=>{
          println("-----------The total number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_num,day,"All",row.getString(0),new JLong(row.getLong(1)))
        })
        product_date_num_array.foreach(row=>{
          println("-----------The total number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_num,day,row.getString(0),row.getString(1),new JLong(row.getLong(2)))
        })

        product_user_num_array.foreach(row=>{
          println("-----------The total user number based on product code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,row.getString(0),"All",new JLong(row.getLong(1)))
        })
        date_user_num_array.foreach(row=>{
          println("-----------The total user number based on date code-----------")
          println(row.getString(0)+" : "+row.getLong(1))
          util.insert(sql_user,day,"All",row.getString(0),new JLong(row.getLong(1)))
        })
        product_date_user_num_array.foreach(row=>{
          println("-----------The total user number based on product&date code-----------")
          println(row.getString(0)+" : "+row.getString(1)+" : "+row.getLong(2))
          util.insert(sql_user,day,row.getString(0),row.getString(1),new JLong(row.getLong(2)))
        })

      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}
