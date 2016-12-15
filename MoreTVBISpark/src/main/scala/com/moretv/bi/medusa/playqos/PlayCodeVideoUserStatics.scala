package com.moretv.bi.medusa.playqos

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 9/7/16.
  */



object PlayCodeVideoUserStatics extends BaseClass {

  private val tableName = "medusa_video_recommend_playqos_playcode"

  private val filterStr = "5iac8sru3fo8,5iv0l79xg64f,fhnoabs9g62c,4g9vh6t9fhgh,3f1cp8npfh5i,5iac9xa1d4vx,5is99x1c5i7p," +
    "fhmoqta1cev0,fh7nv0fg23s9"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(PlayCodeVideoUserStatics, args)
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
            .map(e => (e.getString(0), e.getString(1),e.getString(2)))

//          val tmpRdd = rdd.flatMap(e=>getPlayCode(e._1,e._2,e._3)).filter(e=>isContained(e._2)).
//            map(e=>((e._3,e._4),e._1))
          val tmpRdd = rdd.flatMap(e=>getPlayCode(e._1,e._2,e._3)).filter(e=>isContained(e._2)).
            map(e=>e._1)

//          val sumRddPv = tmpRdd.countByKey()
          val sumRdd =  tmpRdd.distinct.count
          println("The UV is: "+sumRdd)
//          sumRdd.take(100).foreach(println)
//          sumRddPv.take(100).foreach(println)

          cal.add(Calendar.DAY_OF_MONTH,-1)

          if(p.deleteOld){
            util.delete(s"delete from $tableName where day =? ",date)
          }

//          sumRdd.foreach( w => {
//            util.insert(s"insert into $tableName(day,playcode,num)values(?,?,?)",
//              w._1._1,new JLong(w._1._2),new JLong(w._2))
//          })

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

    val res = new ListBuffer[(String, String, String, Int)]()

    try {
      val jsObj = new JSONObject(str)

      val videoSid = jsObj.optString("videoSid")
      val playqosArr = jsObj.optJSONArray("playqos")

      if (playqosArr != null) {

        (0 until playqosArr.length).foreach(i => {
          val playqos = playqosArr.optJSONObject(i)
          val sourcecases = playqos.optJSONArray("sourcecases")

          if (sourcecases != null) {
            (0 until sourcecases.length).foreach(w => {
              val sourcecase = sourcecases.optJSONObject(w)
              res.+=((userId,videoSid,day,groupCode(sourcecase.optInt("playCode"))))
//              res.+=((userId, videoSid, day,sourcecase.optInt("playCode")))
            })
          }
        })
      }
    }
    catch {
      case ex: Exception => {
        res.+=((userId, "", day, 0))
        //throw ex
      }
    }
    res.toList
  }

  def isContained(field:String):Boolean = {
    val splitArr = filterStr.split(",")
    splitArr.contains(field)
  }

  def groupCode(i:Int): Int ={
    i match {
      case -1 =>  -1
      case -2 => -2
      case _ => i
    }
  }
}
