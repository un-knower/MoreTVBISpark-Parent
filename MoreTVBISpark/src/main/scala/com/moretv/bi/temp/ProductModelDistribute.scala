package com.moretv.bi.temp

import java.io.{File, PrintWriter}
import java.util.Calendar

import com.moretv.bi.constant.LogType._
import com.moretv.bi.util.{SparkSetting, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by HuZhehua on 2016/4/12.
 */

object ProductModelDistribute extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
/*        config.set("spark.executor.memory", "8g").
          set("spark.executor.cores", "50").set("spark.cores.max", "200")*/
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
          val logType = LIVE
          val sql = "select date, path, productModel, userId from log_data"
          val df = DFUtil.getDFByDateWithSql(sql,logType,date,11).map(row => {
            val date = row.getString(0)
            val path = row.getString(1)
            val productModel = row.getString(2)
            val userId = row.getString(3)
            if("letvnewc1s".equalsIgnoreCase(productModel) || "m321".equalsIgnoreCase(productModel) ||"we20s".equalsIgnoreCase(productModel) ||
              "magicbox_m13".equalsIgnoreCase(productModel))
              (date, path, productModel.toLowerCase, userId)
            else null
          }).filter(_ != null).toDF("date","path","productModel","userId")

          df.filter("path like 'home-TVlive%'").groupBy("productModel").count().map(row => {
            val model = row.getString(0)
            val count = row.getLong(1).toDouble/11
            (model, count)
          }).toDF("productModel","count").show(200,false)
          df.filter("path like 'home-TVlive%'").distinct().groupBy("productModel").count().map(row => {
            val model = row.getString(0)
            val count = row.getLong(1).toDouble/11
            (model, count)
          }).toDF("productModel","count").show(200,false)

          println("************************************************")
          df.filter("path like 'home-live%'").groupBy("productModel").count().map(row => {
            val model = row.getString(0)
            val count = row.getLong(1).toDouble/11
            (model, count)
          }).toDF("productModel","count").show(200,false)
          df.filter("path like 'home-live%'").distinct().groupBy("productModel").count().map(row => {
            val model = row.getString(0)
            val count = row.getLong(1).toDouble/11
            (model, count)
          }).toDF("productModel","count").show(200,false)
          /*df.map(row => {
            val date = row.getString(0)
            val path = row.getString(1).split("-")
            val productModel = row.getString(2)
            val userId = row.getString(3)
            if(path.length > 1){
              if("home".equals(path(0)) && !path(1).startsWith("%"))
                (date, path(1), productModel, userId)
              else null
            }else null
          }).filter(_ != null).toDF("date","path","productModel","userId").
            groupBy("path","productModel").count().map(row => {
            val path = row.getString(0)
            val model = row.getString(1)
            val count = row.getLong(2).toDouble/11
            (path, model, count)
          }).toDF("path","productModel","count").sort($"path").show(200,false)

          df.map(row => {
            val date = row.getString(0)
            val path = row.getString(1).split("-")
            val productModel = row.getString(2)
            val userId = row.getString(3)
            if(path.length > 1 ){
              if("home".equals(path(0)) && !path(1).startsWith("%"))
                (date, path(1), productModel, userId)
              else null
            }else null
          }).filter(_ != null).toDF("date","path","productModel","userId").distinct().
            groupBy("path","productModel").count().map(row => {
            val path = row.getString(0)
            val model = row.getString(1)
            val count = row.getLong(2).toDouble/11
            (path, model, count)
          }).toDF("path","productModel","count").sort($"path").show(200,false)*/


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

