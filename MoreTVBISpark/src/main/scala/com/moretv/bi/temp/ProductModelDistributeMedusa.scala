package com.moretv.bi.temp

import java.util.Calendar

import com.moretv.bi.constant.LogType._
import com.moretv.bi.util.{SparkSetting, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by HuZhehua on 2016/4/12.
 */

object ProductModelDistributeMedusa extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
     /*   val file = new File("/script/bi/moretv/zhehua/file/temp/PlayviewUserNumByHours.csv")
        val out = new PrintWriter(file)*/
        import sqlContext.implicits._
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
         // val logType = HOMEACCESS
         // val sql = "select date, accessArea, accessLocation, productModel, userId from log_data"
          val df =  sqlContext.read.load("/log/medusa/parquet/201604{02,03,04,05,06,07,08,09,10,11,12}/live").
           select("date","productModel","userId").
           map(row => {
            val date = row.getString(0)
            val productModel = row.getString(1)
            val userId = row.getString(2)
            if("letvnewc1s".equalsIgnoreCase(productModel) || "m321".equalsIgnoreCase(productModel) ||"we20s".equalsIgnoreCase(productModel) ||
              "magicbox_m13".equalsIgnoreCase(productModel))
              (date, productModel.toLowerCase, userId)
            else null
          }).filter(_ != null).toDF("date","productModel","userId")


          df.groupBy("productModel").count().
            map(row => {
            val model = row.getString(0)
            val count = row.getLong(1).toDouble/11
            (model, count)
          }).toDF("productModel","count").show(200,false)
          df.distinct().groupBy("productModel").count().
            map(row => {
            val model = row.getString(0)
            val count = row.getLong(1).toDouble/11
            (model, count)
          }).toDF("productModel","count").show(200,false)

          println("************************************************")

          df.filter("pathMain like 'home*my_tv*collect%'").groupBy("productModel").count().
            map(row => {
            val model = row.getString(0)
            val count = row.getLong(1).toDouble/11
            (model, count)
          }).toDF("productModel","count").show(200,false)
          df.filter("pathMain like 'home*my_tv*collect%'").distinct().groupBy("productModel").count().
            map(row => {
            val model = row.getString(0)
            val count = row.getLong(1).toDouble/11
            (model, count)
          }).toDF("productModel","count").show(200,false)

         /* df.filter("accessArea = 'my_tv' and (accessLocation = 'history' or accessLocation = 'collect')").groupBy("accessLocation","productModel").count().map(row => {
            val location = row.getString(0)
            val model = row.getString(1)
            val count = row.getLong(2).toDouble/11
            (location, model, count)
          }).toDF("accessLocation","productModel","count").sort($"accessLocation").show(200,false)
          df.filter("accessArea = 'my_tv' and (accessLocation = 'history' or accessLocation = 'collect')").distinct().groupBy("accessLocation","productModel").count().map(row => {
            val location = row.getString(0)
            val model = row.getString(1)
            val count = row.getLong(2).toDouble/11
            (location, model, count)
          }).toDF("accessLocation","productModel","count").sort($"accessLocation").show(200,false)
*/
/*          println("************************************************")

          df.filter("accessArea = 'live'").groupBy("productModel").count().map(row => {
            val model = row.getString(0)
            val count = row.getLong(1).toDouble/11
            (model, count)
          }).toDF("productModel","count").show(200,false)
          df.filter("accessArea = 'live'").distinct().groupBy("productModel").count().map(row => {
            val model = row.getString(0)
            val count = row.getLong(1).toDouble/11
            (model, count)
          }).toDF("productModel","count").show(200,false)*/

          cal.add(Calendar.DAY_OF_MONTH,-1)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
    def getRes(df:DataFrame)(implicit sqlContext:SQLContext):DataFrame ={
      df
    }
  }
}

