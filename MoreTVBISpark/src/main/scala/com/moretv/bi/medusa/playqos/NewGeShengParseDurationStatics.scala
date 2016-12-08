package com.moretv.bi.medusa.playqos

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.medusa.playqos.PlayQosInfoByAreaAndISPStatics._
import com.moretv.bi.util.IPLocationUtils.{IPOperatorsUtil, IPLocationDataUtil}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 9/7/16.
  */



object NewGeShengParseDurationStatics extends BaseClass {

  private val tableName = "medusa_play_duration_playcode_by_apk"
  private val tableName1 = "medusa_parse_duration_playcode_by_apk"

  private val filterStr = "tvn8opmne4wx,tvn8p8ijxyab,tvn8p8ijvx2d,tvn88qh6npwx,tvn8p8cd8rab,tvn8opmncdu9,tvn8p8cdrttu,"+
    "tvn88qxzuwtu,tvn8opmnf59v,tvn88qxz8swx,e58r347oe5m7,tvn8qruwoqvw,tvn88qh6abx0,tvn8qruwmo9v,tvn8p8ija212," +
    "tvn8opmn3etu,tvn8p8ij1c2d,5i8sb2m7e5a1,tvn8qruwikab,tvn88qxzoq2d,tvn8p8d3su12,tvn88qh6u9x0,tvn8qruwfhx0," +
    "tvn88qh6wx2d,tvn8p8ijw0tu,tvn88qxztvab,tvn88qxzru2d,tvn8opmna1bc,tvn8qra1klu9,tvn88qh6pqbc,tvn88qh6o8tu," +
    "tvn8p8cdpq2d,tvn8opmnacab,tvn8qruwn8wx,tvn88qh69v2d,5iqt9va1e5xy,tvn8qruw6lbc,tvn8qrxy5hu9,tvn8p8d3uv9v," +
    "tvn8p8ij9wvw,tvn88qxz9xab,tvn8p8ijuv9v,tvn8opmn2ctu,tvn8qruwhjbc,5i8sv0sug6qt,tvn8p8cdqsvw,tvn8qruw7pbc," +
    "5iqt9vbc3fbc,tvn8opmnd3ab,tvn88qh6vwu9,5i8sjkklg6xz,5i8s1bnoce5h,5i8sac5h5iwx,tvn88qxzs9wx,tvn88qxzprwx," +
    "tvn8p8cdo89v,tvn8opmn4fu9,tvn88qxzqtab,tvn8qruw5i9v,tvn8opmn1btu,tvn8qruwprx0,tvn8opmnb29v,5iqtqrbd4g6l," +
    "5iqtvw7p5ief,fhbd9vbd23ik,5iqtwxn8d45h"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(NewGeShengParseDurationStatics, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = new DBOperationUtils("medusa")

        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        sqlContext.udf.register("getProvince",IPLocationDataUtil.getProvince _)
        sqlContext.udf.register("getISP",IPOperatorsUtil.getISPInfo _)
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

          val rdd = sqlContext.read.parquet(readPath).select("userId","date", "jsonLog","ip","productModel","apkVersion")
            .map(e => (e.getString(0), e.getString(1),e.getString(2),
            IPLocationDataUtil.getProvince(e.getString(3)),
              IPOperatorsUtil.getISPInfo(e.getString(3)),e.getString(4),e.getString(5)))

          val tmpRdd = rdd.flatMap(e=>getPlayCode(e._1,e._2,e._3,e._4,e._5,e._6,e._7)).filter(e=>isContained(e._2)).
            map(e=>(e._3,e._4,e._5,e._6,e._1,e._7,e._8,e._9,e._10))

          val apkVersionPlayDurationInfo = tmpRdd.filter(_._4 == -1).map(e=>((e._3,e._9),e._5))
          val playDurationNum = apkVersionPlayDurationInfo.countByKey()
          val playDurationUser = apkVersionPlayDurationInfo.distinct().countByKey()

          val apkVersionParseDurationInfo = tmpRdd.filter(_._4 == -1).map(e=>((e._2,e._9),e._5))
          val parseDurationNum = apkVersionParseDurationInfo.countByKey()
          val parseDurationUser = apkVersionParseDurationInfo.distinct().countByKey()

          val insertSql1 = s"insert into $tableName(playDuration,apkVersion,playDurationNum,playDurationUser) values (?,?,?,?)"
          val insertSql2 = s"insert into $tableName1(parseDuration,apkVersion,parseDurationNum,parseDurationUser) values (?,?,?,?)"
          playDurationNum.foreach(i=>{
            val key = i._1
            val user = playDurationUser.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            util.insert(insertSql1,new JLong(i._1._1),i._1._2,new JLong(i._2),new JLong(user))
          })
          parseDurationNum.foreach(i=>{
            val key = i._1
            val user = parseDurationUser.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            util.insert(insertSql2,new JLong(i._1._1),i._1._2,new JLong(i._2),new JLong(user))
          })

//          val initBufferDurationInfo = tmpRdd.filter(_._4 == -1).map(e=>((e._3,e._6,e._7,e._8),e._5))
//          val playDurationNum = initBufferDurationInfo.countByKey()
//          val playDurationUser = initBufferDurationInfo.distinct().countByKey()
//          val insertSql1 = s"insert into $tableName(playDuration,area,isp,product,playDurationNum,playDurationUser) values (?,?,?,?,?,?)"
//          playDurationNum.foreach(i=>{
//            val key = i._1
//            val user = playDurationUser.get(key) match {
//              case Some(e) => e
//              case _ => 0L
//            }
//            util.insert(insertSql1,new JLong(i._1._1),i._1._2,i._1._3,i._1._4,new JLong(i._2),new JLong(user))
//          })
//          val parseDurationInfo = tmpRdd.filter(_._4 == -1).map(e=>((e._2,e._6,e._7,e._8),e._5))
//          val parseDurationNum = parseDurationInfo.countByKey()
//          val parseDurationUser = parseDurationInfo.distinct().countByKey()
//          val insertSql2 = s"insert into $tableName1(parseDuration,area,isp,product,parseDurationNum,parseDurationUser) values (?,?,?,?,?,?)"
//          parseDurationNum.foreach(i=>{
//            val key = i._1
//            val user = parseDurationUser.get(key) match {
//              case Some(e) => e
//              case _ => 0L
//            }
//            util.insert(insertSql2,new JLong(i._1._1),i._1._2,i._1._3,i._1._4,new JLong(i._2),new JLong(user))
//          })
//          val initBufferDurationInfoByArea = tmpRdd.filter(_._4 == -1).map(e=>((e._3,e._6),e._5))
//          val playDurationNumByArea = initBufferDurationInfoByArea.countByKey()
//          val playDurationUserByArea = initBufferDurationInfoByArea.distinct().countByKey()
//          playDurationNumByArea.foreach(i=>{
//            val key = i._1
//            val user = playDurationUserByArea.get(key) match {
//              case Some(e) => e
//              case _ => 0L
//            }
//            util.insert(insertSql1,new JLong(i._1._1),i._1._2,"All","All",new JLong(i._2),new JLong(user))
//          })
//          val parseDurationInfoByArea = tmpRdd.filter(_._4 == -1).map(e=>((e._2,e._6),e._5))
//          val parseDurationNumByArea = parseDurationInfoByArea
//          val parseDurationUserByArea = parseDurationInfoByArea.distinct().countByKey()
//          parseDurationNumByArea.foreach(i=>{
//            val key = i._1
//            val user = parseDurationUserByArea.get(key) match {
//              case Some(e) => e
//              case _ => 0L
//            }
//            util.insert(insertSql2,new JLong(i._1._1),i._1._2,"All","All",new JLong(i._2),new JLong(user))
//          })
//
//          val initBufferDurationInfoByAreaISP = tmpRdd.filter(_._4 == -1).map(e=>((e._3,e._6,e._7),e._5))
//          val playDurationNumByAreaISP = initBufferDurationInfoByAreaISP.countByKey()
//          val playDurationUserByAreaISP = initBufferDurationInfoByAreaISP.distinct().countByKey()
//          playDurationNumByAreaISP.foreach(i=>{
//            val key = i._1
//            val user = playDurationUserByAreaISP.get(key) match {
//              case Some(e) => e
//              case _ => 0L
//            }
//            util.insert(insertSql1,new JLong(i._1._1),i._1._2,i._1._3,"All",new JLong(i._2),new JLong(user))
//          })
//          val parseDurationInfoByAreaISP = tmpRdd.filter(_._4 == -1).map(e=>((e._2,e._6,e._7),e._5))
//          val parseDurationNumByAreaISP = parseDurationInfoByAreaISP.countByKey()
//          val parseDurationUserByAreaISP = parseDurationInfoByAreaISP.distinct().countByKey()
//          parseDurationNumByAreaISP.foreach(i=>{
//            val key = i._1
//            val user = parseDurationUserByAreaISP.get(key) match {
//              case Some(e) => e
//              case _ => 0L
//            }
//            util.insert(insertSql2,new JLong(i._1._1),i._1._2,i._1._3,"All",new JLong(i._2),new JLong(user))
//          })



//          val playDurationInfo = tmpRdd.filter(_._4 == -1).map(e=>(e._3,e._5))
//          val playDurationNum = playDurationInfo.countByKey()
//          val playDurationUser = playDurationInfo.distinct().countByKey()
//          println("==========Play Duration Info============")
////          playDurationNum.foreach(println)
////          playDurationUser.foreach(println)
//
//          val parseDurationInfo = tmpRdd.filter(_._4 == -1).map(e=>(e._2,e._5))
//          val parseDurationNum = parseDurationInfo.countByKey()
//          val parseDurationUser = parseDurationInfo.distinct().countByKey()
//          println("==========Parse Duration Info============")
////          parseDurationNum.foreach(println)
////          parseDurationUser.foreach(println)
//          val insertSql1 = s"insert into $tableName(playDuration,playDurationNum,playDurationUser) values (?,?,?)"
//          playDurationNum.foreach(i=>{
//            val key = i._1
//            val user = playDurationUser.get(key) match {
//              case Some(e) => e
//              case _ => 0L
//            }
//            util.insert(insertSql1,new JLong(i._1),new JLong(i._2),new JLong(user))
//          })
//
//          val insertSql2 = s"insert into $tableName1(parseDuration,parseDurationNum,parseDurationUser) values (?,?,?)"
//          parseDurationNum.foreach(i=>{
//            val key = i._1
//            val user = parseDurationUser.get(key) match {
//              case Some(e) => e
//              case _ => 0L
//            }
//            util.insert(insertSql2,new JLong(i._1),new JLong(i._2),new JLong(user))
//          })



          cal.add(Calendar.DAY_OF_MONTH,-1)

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
    * @return (userId, videoSid, day, parseDuration,playDuration,playcode)
    */
  def getPlayCode(userId: String,  day: String, str: String,area:String,isp:String,product:String,apkVersion:String) = {

    val res = new ListBuffer[(String, String, String,Long,Long, Int,String,String,String,String)]()

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
              res.+=((userId,videoSid,day,sourcecase.optLong("parseDuration"),sourcecase.optLong("initBufferDuration"),
                groupCode(sourcecase.optInt("playCode")),area,isp,product,apkVersion))
            })
          }
        })
      }
    }
    catch {
      case ex: Exception => {
        res.+=((userId, "", day,0L,0L, 0,"","","",""))
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
