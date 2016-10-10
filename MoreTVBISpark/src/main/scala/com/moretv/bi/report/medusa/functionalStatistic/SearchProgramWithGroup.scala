package com.moretv.bi.report.medusa.functionalStatistic

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.constant.ApkVersion
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.immutable.ListMap

/**
  * Created by witnes on 9/14/16.
  */

/**
  * 更换新数据源 (clickResult)
  */
object SearchProgramWithGroup extends  BaseClass{

  private val tableName = "searchprogram_withgroup_freq"


  def main(args: Array[String]):Unit = {

    ModuleClass.executor(SearchProgramWithGroup,args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {
        //init util
        val util = new DBOperationUtils("medusa")

        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(w => {

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path 
          val path = s"/log/medusa/parquet/$loadDate/clickResult"
          println(path)

          try{
            //df 
            sqlContext.read.parquet(path).registerTempTable("log")

            val avaiableVersion = ApkVersion.availableMedusaApkVersionStr
            val df = sqlContext.sql(
              "select userId, params['resultSid'] as resultSid," +
              "params['resultName'] as resultName," +
              s" contentType,apkVersion from log where  apkVersion in ($avaiableVersion)")
                  .filter("contentTYpe is not null")

            //rdd((resultSid,resultName,contentType),userId)
            val rdd = df.map(e=>((e.getString(1),e.getString(2),e.getString(3)),e.getString(0)))

            val uvMap = rdd.distinct.countByKey
            val pvMap = rdd.countByKey

            //deal with table
            util.delete(s"delete from $tableName where day= ?",sqlDate)
            uvMap.foreach(w=>{
              val key = w._1
              val pv = pvMap.get(key) match {
                case Some(p) => p
                case _ => 0
              }
              util.insert(s"insert into $tableName(day,resultSid,resultName,cABTest.jarontentType,search_user,search_num)values(?,?,?,?,?,?)",
                sqlDate, w._1._1,w._1._2,w._1._3,new JLong(w._2),new JLong(pv))
            })

          }catch {
            case ex:Exception => println(ex)
          }


        })

      }
      case None => {
        throw new Exception("at least one param for SearchProgramWithGroup")
      }
    }

  }
}
