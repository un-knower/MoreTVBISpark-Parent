package com.moretv.bi.report.medusa.util

import java.util.Calendar
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.lang.{Long=>JLong}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2016/4/14.
 */
object StatisticsModel {

  def pvuvStatisticModel(args:Array[String],sqlContext: SQLContext,logType:String,countBy:String,insertTable:String,
                         sqlInsert:String)={
    val util = new DBOperationUtils("medusa")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val df = sqlContext.read.parquet(s"/log/medusa/parquet/$date/$logType/").cache()
          df.registerTempTable("medusa_log")
          val pvSql = s"select count($countBy) from medusa_log"
          val pvDf = sqlContext.sql(pvSql)
          val uvSql = s"select count(distinct $countBy) from medusa_log"
          val uvDf = sqlContext.sql(uvSql)
          val pv = pvDf.map(e=>e.getLong(0)).first()
          val uv = uvDf.map(e=>e.getLong(0)).first()
          /*转换成数据库中的日期格式*/
          val day = DateFormatUtils.toDateCN(date,-1)

          if(p.deleteOld){
            val sqlDelete = s"delete from $insertTable where day = ?"
            util.delete(sqlDelete,day)
          }

          util.insert(sqlInsert,day,new JLong(pv),new JLong(uv))
          cal.add(Calendar.DAY_OF_MONTH, -1)
          df.unpersist()
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }
  }

  def pvuvRestrictStatisticModel(args:Array[String],sqlContext: SQLContext,logType:String,event:String,
                                 statisticType:String="",
                                 countBy:String,restrictColumnContent:Array[String],
                                 insertTable:String, sqlInsert:String,countByColumnName:String,
                                 restrictByColumnName:String,eventColumnName:String,statisticByColumnName:String="")={
    val util = new DBOperationUtils("medusa")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val df = sqlContext.read.parquet(s"/log/medusa/parquet/$date/$logType").cache()

          if (statisticType==""){
            val rdd = df.select(countByColumnName,restrictByColumnName,eventColumnName).map(e=>(e.getString(0),e.getString(1),e.getString(2))).filter(_._3==event)
            (0 until restrictColumnContent.length).foreach(i=>{

              val filterRdd = rdd.filter(_._1!=null).filter(_._2!=null).map(e=>(e._1,e._2)).filter(_._2.contains
                (restrictColumnContent(i))).map(e=>e._1)

              val pv = filterRdd.count()
              val uv = filterRdd.distinct().count()
              val area_name = MedusaLogInfoUtil.identifyNameMapping(restrictColumnContent(i))

              val day = DateFormatUtils.toDateCN(date,-1)

              if(p.deleteOld){
                val sqlDelete = s"delete from $insertTable where day = ?"
                util.delete(sqlDelete,day)
              }
              util.insert(sqlInsert,day,restrictColumnContent(i),area_name,new JLong(pv),new JLong(uv))

              df.unpersist()
            })
          }else{
            /*需要预先过滤统计的目标*/
            val rdd = df.select(countByColumnName,restrictByColumnName,eventColumnName,statisticByColumnName).map(e=>(e
              .getString(0),e.getString(1),e.getString(2),e.getString(3))).filter(_._3!=null).filter(_._3.contains(event))
              .filter(_._4!=null).filter(_._4.contains(statisticType))
            (0 until restrictColumnContent.length).foreach(i=>{

              val filterRdd = rdd.filter(_._1!=null).filter(_._2!=null).map(e=>(e._1,e._2)).filter(_._2.contains
                (restrictColumnContent(i))).map(e=>(e._1,1))

              val pv = filterRdd.count()
              val uv = filterRdd.distinct().count()
              val area_name = MedusaLogInfoUtil.identifyNameMapping(restrictColumnContent(i))
              val day = DateFormatUtils.toDateCN(date,-1)

              if(p.deleteOld){
                val sqlDelete = s"delete from $insertTable where day = ?"
                util.delete(sqlDelete,day)
              }
              util.insert(sqlInsert,day,restrictColumnContent(i),area_name,new JLong(pv),new JLong(uv))
              df.unpersist()
            })
          }
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }
  }

  def pvuvByHourStatisticModel(args:Array[String],sqlContext:SQLContext,logType:String,event:String,
                               statisticType:String="",
                               countBy:String,restrictColumnContent:Array[String],
                               insertTable:String, sqlInsert:String,countByColumnName:String,
                               restrictByColumnName:String,dateTimeColumnName:String,eventColumnName:String,statisticByColumnName:String="")={
    val util = new DBOperationUtils("medusa")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val df = sqlContext.read.parquet(s"/log/medusa/parquet/$date/$logType").cache()

          if (statisticType==""){
            val rdd = df.select(countByColumnName,restrictByColumnName,dateTimeColumnName,eventColumnName).map(e=>(e
              .getString(0),e.getString(1),e.getString(2),e.getString(3))).filter(_._4==event)
            (0 until restrictColumnContent.length).foreach(i=>{

              val filterRdd = rdd.map(e=>(e._1,e._2,e._3)).filter(_._2!=null).filter(_._2.contains(restrictColumnContent
                (i))).filter(_._3!=null).map(e=>(e._3.substring(11,13),e._1))
              val pv = filterRdd.countByKey().toArray
              val uv = filterRdd.distinct().countByKey().toArray
              val area_name = MedusaLogInfoUtil.identifyNameMapping(restrictColumnContent(i))

              val day = DateFormatUtils.toDateCN(date,-1)

              if(p.deleteOld){
                val sqlDelete = s"delete from $insertTable where day = ?"
                util.delete(sqlDelete,day)
              }
              (0 until pv.length).foreach(k=>{
                util.insert(sqlInsert,day,restrictColumnContent(i),area_name,pv(k)._1,new JLong(pv(k)._2),new JLong(uv
                  (k)._2))
              })
              df.unpersist()
            })
          }else{
            /*需要预先过滤统计的目标*/
            val rdd = df.select(countByColumnName,restrictByColumnName,dateTimeColumnName,eventColumnName,statisticByColumnName).map(e=>(e
              .getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4))).filter(_._4.contains(event))
              .filter(_._5.contains(statisticType))
            (0 until restrictColumnContent.length).foreach(i=>{

              val filterRdd = rdd.map(e=>(e._1,e._2)).filter(_._2!=null).filter(_._2.contains(restrictColumnContent(i)))
                .map(e=>(e._1,1))

              val pv = filterRdd.count()
              val uv = filterRdd.distinct().count()
              val area_name = MedusaLogInfoUtil.identifyNameMapping(restrictColumnContent(i))
              val day = DateFormatUtils.toDateCN(date,-1)

              if(p.deleteOld){
                val sqlDelete = s"delete from $insertTable where day = ?"
                util.delete(sqlDelete,day)
              }
              util.insert(sqlInsert,day,restrictColumnContent(i),area_name,new JLong(pv),new JLong(uv))

              df.unpersist()
            })
          }
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }
  }

  def sumStatisticModel(args:Array[String],sqlContext:SQLContext,logType:String,sumBy:String,insertTable:String,
                         sqlInsert:String)={
    val util = new DBOperationUtils("medusa")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val df = sqlContext.read.parquet(s"/log/medusa/parquet/$date/$logType/").cache()
          df.registerTempTable("medusa_log")
          val sumSql = s"select sum($sumBy) from medusa_log"
          val sumDf = sqlContext.sql(sumSql)
          val sum = sumDf.map(e=>e.getLong(0)).first()
          /*转换成数据库中的日期格式*/
          val day = DateFormatUtils.toDateCN(date,-1)

          if(p.deleteOld){
            val sqlDelete = s"delete from $insertTable where day = ?"
            util.delete(sqlDelete,day)
          }

          util.insert(sqlInsert,day,new JLong(sum))
          cal.add(Calendar.DAY_OF_MONTH, -1)
          df.unpersist()
        })
      }
      case None => {throw new RuntimeException("At needs the param: startDate!")}
    }
  }

}
