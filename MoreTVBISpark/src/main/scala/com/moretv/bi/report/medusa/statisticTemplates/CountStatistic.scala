package com.moretv.bi.report.medusa.statisticTemplates

/**
 * Created by Xiajun on 2016/5/3.
 */

import java.sql.DriverManager
import java.util.Calendar

import com.moretv.bi.constant.Constants._
import com.moretv.bi.report.medusa.util.{DFUtil, UDFSets}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import com.moretv.bi.util.ImplicitClass._
import java.lang.{Long => JLong}

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases

import scala.util.parsing.json.{JSON, JSONType}

/*此函数用于统计需要使用count类型的统计需求*/
object CountStatistic extends BaseClass{

  def main(args: Array[String]): Unit = {
    //   JobStatus.getConfig(appName)
    ModuleClass.executor(this,args)

  }
  override def execute(args: Array[String]): Unit = {
    /*从数据库中读取相应的app_name的配置参数*/

    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

//   UDFSets.getPathMainInfo _这样的做法是将调用者被调用时传入参数
    sqlContext.udf.register("getPathMainInfo",UDFSets.getPathMainInfo _)
    val minId = 1
    val maxId = util.selectOne("select max(id) from statistic_app_config_info")(0).toString.toLong
    val numOfPartition = 20

    val configRdd=DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
      .getJdbcRDD(sc,s"select app_name,params_config,template,logType from statistic_app_config_info where id >= ${minId} and id <= ${maxId} and " +
      "template='CountStatistic'","statistic_app_config_info"
      ,r=>(r.getString(1),r.getString(2),r.getString(3),r.getString(4))
      ,numOfPartition)

    println("=====================================")

    val configRddByLogType = configRdd.map(e=>(e._4,(e._1,e._2,e._3))).groupByKey()
    println("The length of configRddByLogType is: "+configRddByLogType.collect().length)
    configRddByLogType.collect().foreach(i=>{
      println("Based on the logType!")
      /*过滤出所有需要使用当前template进行统计的app_name的参数*/
      val configRddCollect = i._2
      var firstConfig = ""
      /*定时任务，所有的app的时间都是相同的*/
      configRddCollect.foreach(e=>{
        firstConfig = e._2
        println("The final config is: "+firstConfig)
      })
      val jsonObj = JSON.parseFull(firstConfig)
      val map:Map[String,Any] = jsonObj.get.asInstanceOf[Map[String,Any]]
      val startDate = map.get("startDate").get.toString
      val logType = map.get("logType").get.toString
      val fileDir = map.get("fileDir").get.toString
      val applicationMode = map.get("applicationMode").get.toString
      /*开始进行相关处理过程*/
      val logData = DFUtil.getOriginalDFByDate(logType,startDate,fileDir,applicationMode).persist(StorageLevel.MEMORY_AND_DISK)

      configRddCollect.foreach(e=>{
        /*对json字符串进行解析*/
        println("===============================Begin===================")
        val jsonObj = JSON.parseFull(e._2)
        val map:Map[String,Any] = jsonObj.get.asInstanceOf[Map[String,Any]]
        println("The config length is: "+map.size)
        /*获取参数中的所有参数值*/
        val startDate = map.get("startDate").get.toString
        val numOfDays = map.get("numOfDays").get.toString
        val logType = map.get("logType").get.toString
        val fileDir = map.get("fileDir").get.toString
        val deleteOld = map.get("deleteOld").get.toString
        val applicationMode = map.get("applicationMode").get.toString
        val columns = map.get("columns").get.toString
        val sqlSpark = map.get("sqlSpark").get.toString
        val sqlInsert = map.get("sqlInsert").get.toString
        val tableName = map.get("tableName").get.toString
        val dbMode = map.get("dbMode").get.toString

        /*获得datafame*/
        val df = if(columns.nonEmpty){
          val columnsInfo = columns.split(COLUMN_SEPARATOR)
          if(columnsInfo.size == 1){
            logData.select(columnsInfo(0))
          }else {
            val cs = columnsInfo.splitAt(1)
            logData.select(cs._1(0),cs._2:_*)
          }
        }else logData

        df.registerTempTable("log_data")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        cal.add(Calendar.DAY_OF_MONTH,-1)

        (0 until Integer.parseInt(numOfDays)).foreach(i=>{
          val resultDF = sqlContext.sql(sqlSpark).collectAsList()
          //Medusa与moretv的log日期实际上记录的是前一天的数据
          val day = DateFormatUtils.readFormat.format(cal.getTime)
          val date = DateFormatUtils.toDateCN(day)

          //        创建数据库
          val db = new DBOperationUtils(dbMode)
          val sqlDelete = s"delete from $tableName where day = ?"
          //        删除数据
          if(deleteOld=="true") db.delete(sqlDelete,date)

          //        插入数据库
          resultDF.foreach(row=>{
            row.size match {
              case 1 => {
                val params = row.toTuple1
                db.insert(sqlInsert,date,params)
              }
              case 2 => {
                val params = row.toTuple2
                db.insert(sqlInsert,date,params._1,params._2)
              }
              case 3 => {
                val params = row.toTuple3
                db.insert(sqlInsert,date,params._1,params._2,params._3)
              }
              case 4 => {
                val params = row.toTuple4
                db.insert(sqlInsert,date,params._1,params._2,params._3,params._4)
              }
            }

          })
          cal.add(Calendar.DAY_OF_MONTH,-1)
        })
      })
      logData.unpersist()
    })
  }
}
