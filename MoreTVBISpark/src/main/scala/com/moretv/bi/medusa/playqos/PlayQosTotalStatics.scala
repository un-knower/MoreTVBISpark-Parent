package com.moretv.bi.medusa.playqos

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 9/7/16.
  */


/**
  * 一段时间内所有 播放质量的 uv 统计
  */
object PlayQosTotalStatics extends BaseClass {

  private val tableName = "medusa_playqos_total"

  //private val tableName = "medusa_total_playqos"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(PlayQosTotalStatics, args)
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
          cal.add(Calendar.DAY_OF_MONTH,-1)


          //临时没有该parquet文件,容错
          if(date.equals("20160815")){
            readPath = s"/log/medusa/parquet/20160814/playqos"
            cal.add(Calendar.DAY_OF_MONTH,-1)
          }
          else{
            readPath = s"/log/medusa/parquet/$date/playqos"
          }

          val date1 = DateFormatUtils.cnFormat.format(cal.getTime)

          println(readPath)

          val rdd = sqlContext.read.parquet(readPath).select("userId","date", "jsonLog","apkVersion")
                    .filter(s"date='$date1'")
                    .map(e => (e.getString(0), e.getString(1),e.getString(2),e.getString(3)))

//        //((day,apkVersion),num)
//          val tmpRdd = rdd.flatMap(e=>getPlayCode(e._1,e._2,e._3,e._4))
//                            .map(e=>((e._3,e._4,e._5),e._1))


          //((day,playcode,apkVersion),num)
          val tmpRdd = rdd.flatMap(e=>getPlayCode(e._1,e._2,e._3,e._4))
                            .map(e=>((e._3,e._4,e._5),e._1))

          val sumRdd =  tmpRdd.distinct.countByKey()

          sumRdd.take(100).foreach(println)

          if(p.deleteOld){
            util.delete(s"delete from $tableName where day =? ",date1)
          }

          sumRdd.foreach( w => {

//            util.insert(s"insert into $tableName(day,apkVersion,num)values(?,?,?)",
//              w._1._1,w._1._3,new JLong(w._2))

              util.insert(s"insert into $tableName(day,playcode,apkVersion,num)values(?,?,?,?)",
                w._1._1, new JLong(w._1._2),w._1._3,new JLong(w._2))
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
    * @param str json字符串
    * @return (userId, videoSid, day, playcode, apkVersion)
    */
  def getPlayCode(userId: String,  day: String, str: String, apkVersion: String) = {

    val res = new ListBuffer[(String, String, String, Int, String)]()

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
              //res.+=((videoSid, day,groupCode(sourcecase.optInt("playCode"))))
              res.+=((userId, videoSid, day,sourcecase.optInt("playCode"), apkVersion))
            })
          }
        })
      }
    }
    catch {
      case ex: Exception => {
        res.+=((userId, "", day, 0, apkVersion))
        //throw ex
      }
    }
    res.toList
  }

}
