package com.moretv.bi.medusa.playqos

import java.util.Calendar
import java.lang.{Long => JLong}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.json.JSONObject
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONArray

/**
  * Created by witnes on 9/7/16.
  */

/**
  * playqos vv 统计
  */
object PlayCodeStatics extends BaseClass{

  private val tableName = "medusa_playqos_playcode"

  def main(args: Array[String]) {
    ModuleClass.executor(PlayCodeStatics,args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        var readPath = ""
        (0 until p.numOfDays).foreach(i => {

          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)

          if(date.equals("20160815")){
            readPath = s"/log/medusa/parquet/20160814/playqos"
            cal.add(Calendar.DAY_OF_MONTH,-1)
          }
          else{
            readPath = s"/log/medusa/parquet/$date/playqos"
          }
          val date1 = DateFormatUtils.cnFormat.format(cal.getTime)

          println(readPath)

          val rdd =  sqlContext.read.parquet(readPath).select("date","jsonLog")
                        .filter(s"date=$date1")
                        .map(e=> (e.getString(0), e.getString(1)))

          val tmpRdd = rdd.flatMap(e=>getPlayCode(e._1,e._2))
                          .map(w=>((w._1,w._2),1))

          val sumRdd =  tmpRdd.reduceByKey(_+_)


          if(p.deleteOld){
            util.delete(s"delete from $tableName where day =? ",date1)
          }

          sumRdd.collect.foreach( w => {
            util.insert(s"insert into $tableName(day,playcode,num)values(?,?,?)",
              w._1._1,new JLong(w._1._2),new JLong(w._2))
          })

        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }

  }

  /**
    *
    * @param day
    * @param str
    * @return
    */
  def getPlayCode(day:String, str: String) ={

    val res = new ListBuffer[(String,Int)]()

    try{
      val jsObj = new JSONObject(str)
      val playqosArr = jsObj.optJSONArray("playqos")

      if( playqosArr!=null ){

        (0 until playqosArr.length).foreach(i=>{
          val playqos = playqosArr.optJSONObject(i)
          val sourcecases = playqos.optJSONArray("sourcecases")

          if(sourcecases != null){
            (0 until sourcecases.length).foreach(w=>{
              val sourcecase = sourcecases.optJSONObject(w)
              res.+=((day,sourcecase.optInt("playCode")))
            })
          }

        })
      }

    }catch {
      case ex:Exception=> {
        res  .+=((day,0))
        //throw ex
      }
    }
    res.toList
  }

}
