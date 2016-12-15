package com.moretv.bi.medusa.playqos

import java.io.{FileOutputStream, OutputStreamWriter}
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



object PlayCodeVideoSourceStaticsTest extends BaseClass {

//  private val tableName = "medusa_video_recommend_playqos_playcode"
  private val tableName = "medusa_video_playqos_playcode_source_copy"

//  private val filterStr = "tvn8sts934tu,tvn8sts9ef12,tvn8sts945u9,tvn8sts9fg12,tvn8sts95hbc,tvn8sts9gibc," +
//    "tvn8sts9h62d,tvn8sts9ij12,tvn8sts96kab,tvn8sts9jlbc,tvn8sts9rtx0,tvn8sttvxy12"
private val filterStr = "5iac8sru3fo8,5iv0l79xg64f,fhnoabs9g62c,4g9vh6t9fhgh,3f1cp8npfh5i,5iac9xa1d4vx,5is99x1c5i7p," +
  "fhmoqta1cev0,fh7nv0fg23s9"


//  private val filterStr = "s9n8tu45vx2d,s9n8tuc3rs12,9w9vk7tvxz34,8rqr5ioq4fm7,e534qrcefh1c," +
//    "s9n8uv6jtv2d,fhhj5gf5g6pr,8rqr5ib2x0ce,fhik452cd46j,vxnptu2ccdst,e5c36kxyhj9w,xy5gu9jm9wc3," +
//    "vxnp9wacfho8,9wfg5gcev0t9,vxnptuoqjkd3,9wabsu4fgiln,e5c36lvwd4m7,8rqrlmno7pde,s9n8tu34ijx0," +
//    "4ghimo5g4gpq,xy5gu9lnjlbd,s9n8tu12t9ab,w012a2lnb2a1,s9n8tuc38qu9,fhik45f5fhwy,e5de7ojk23jm," +
//    "8rqrkli6t9wy,8rqr6j8ra24g,9wqr9wd3ijx0,9wqrpqjmabbc,s9n8tu12a2ab,e5detvmocevx,vxnp9w1blm23," +
//    "vxoqlmoq4545,xy12u9klm71b"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(PlayCodeVideoSourceStaticsTest, args)
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
          val cday = cal.getTime
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

          val tmpRdd = rdd.flatMap(e=>getPlayCode(e._1,e._2,e._3)).filter(e=>isContained(e._2)).
            map(e=>((e._3,e._4,e._5),e._1))
//          val tmpRdd = rdd.flatMap(e=>getPlayCode(e._1,e._2,e._3)).map(e=>((e._3,e._4,e._5),e._1)).cache()
          val numRdd = tmpRdd.countByKey()
          val sumNum = tmpRdd.map(e=>e._2).count()
          val sourceNum = tmpRdd.map(e=>((e._1._1,e._1._2),e._2)).countByKey()

          if(p.deleteOld){
            val deleteSql = s"delete $tableName where day = ?"
            util.delete(deleteSql,insertDate)
          }
          val insertSql = s"insert into $tableName(day,videoSid,source,playcode,num,sourceNum,totalNum) values(?,?,?,?,?,?,?)"
          numRdd.foreach(i=>{
            val key = (i._1._1,i._1._2)
            val eachSourceNum = sourceNum.get(key) match {
              case Some(e) => e
              case None => 0L
            }
            util.insert(insertSql,cday,"杀人不眨眼的信用卡",i._1._2,new JLong(i._1._3),new JLong(i._2),new JLong(eachSourceNum),new JLong(sumNum))
          })

//          val sumRdd =  tmpRdd.distinct.countByKey()

//          sumRdd.take(100).foreach(println)



//          if(p.deleteOld){
//            util.delete(s"delete from $tableName where day =? ",date)
//          }

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

    val res = new ListBuffer[(String, String, String,String, Int)]()

    try {
      val jsObj = new JSONObject(str)

      val videoSid = jsObj.optString("videoSid")
      val playqosArr = jsObj.optJSONArray("playqos")

      if (playqosArr != null) {

        (0 until playqosArr.length).foreach(i => {
          val playqos = playqosArr.optJSONObject(i)
          val source = playqos.optString("videoSource")
          val sourcecases = playqos.optJSONArray("sourcecases")

          if (sourcecases != null) {
            (0 until sourcecases.length).foreach(w => {
              val sourcecase = sourcecases.optJSONObject(w)
              res.+=((userId,videoSid,day,source,groupCode(sourcecase.optInt("playCode"))))
            })
          }
        })
      }
    }
    catch {
      case ex: Exception => {
        res.+=((userId, "", day,"", 0))
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
