package com.moretv.bi.medusa.playqos

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, ProgramRedisUtil}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 9/7/16.
  */



object PlayCodeVideoContentSourceStatics extends BaseClass {

  private val tableName = "medusa_video_content_type_playqos_playcode_source"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        var readPath =""
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          //临时没有该parquet文件,容错
          if(date.equals("20160815")){
            readPath = s"/log/medusa/parquet/20160814/playqos"
            cal.add(Calendar.DAY_OF_MONTH,-1)
          }
          else{
            readPath = s"/log/medusa/parquet/$date/playqos"
          }
          println(readPath)

          val rdd = sqlContext.read.parquet(readPath).select("userId","date", "jsonLog")
            .map(e => (e.getString(0), e.getString(1),e.getString(2))).filter(_._2==insertDate)

          val tmpRdd = rdd.flatMap(e=>getPlayCode(e._1,e._2,e._3)).map(e=>((e._2,e._3,e._4,e._5,e._6),e._1)).cache()
          val numRdd = tmpRdd.countByKey()
          val sourceNum = tmpRdd.map(e=>((e._1._1,e._1._2,e._1._3,e._1._5),e._2)).countByKey()
          tmpRdd.unpersist()

          if(p.deleteOld){
            val deleteSql = s"delete from $tableName where day = ?"
            util.delete(deleteSql,insertDate)
          }
          val insertSql = s"insert into $tableName(day,videoSid,title,source,playcode,contentType,num,sourceNum) values(?,?,?,?,?,?,?,?)"
          numRdd.foreach(i=>{
            val key = (i._1._1,i._1._2,i._1._3,i._1._5)
            val eachSourceNum = sourceNum.get(key) match {
              case Some(e) => e
              case None => 0L
            }
            util.insert(insertSql,insertDate,i._1._1,ProgramRedisUtil.getTitleBySid(i._1._1),i._1._2,new JLong(i._1._4),i._1._5,new JLong(i._2),new JLong(eachSourceNum))
          })

          cal.add(Calendar.DAY_OF_MONTH,-1)
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
    * @param str json字符串
    * @return (userId, videoSid, day, playcode)
    */
  def getPlayCode(userId: String,  day: String, str: String) = {

    val res = new ListBuffer[(String, String, String,String, Int,String)]()

    try {
      val jsObj = new JSONObject(str)

      val videoSid = jsObj.optString("videoSid")
      val contentType = jsObj.optString("contentType")
      val playqosArr = jsObj.optJSONArray("playqos")

      if (playqosArr != null) {

        (0 until playqosArr.length).foreach(i => {
          val playqos = playqosArr.optJSONObject(i)
          val source = playqos.optString("videoSource")
          val sourcecases = playqos.optJSONArray("sourcecases")

          if (sourcecases != null) {
            (0 until sourcecases.length).foreach(w => {
              val sourcecase = sourcecases.optJSONObject(w)
              res.+=((userId,videoSid,day,source,groupCode(sourcecase.optInt("playCode")),contentType))
            })
          }
        })
      }
    }
    catch {
      case ex: Exception => {
        res.+=((userId, "", day,"", 0,""))
        //throw ex
      }
    }
    res.toList
  }

//  def isContained(field:String):Boolean = {
//    val splitArr = filterStr.split(",")
//    splitArr.contains(field)
//  }

  def groupCode(i:Int): Int ={
    i match {
      case -1 =>  -1
      case -2 => -2
      case _ => i
    }
  }
}
