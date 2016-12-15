package com.moretv.bi.medusa.playqos

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.util.IPLocationUtils.{IPOperatorsUtil, IPLocationDataUtil}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 9/7/16.
  */

object PlayInfoByAreaAndISPStatics extends BaseClass {

  private val tableName1 = "medusa_playinfo_area_playqos"
  private val tableName2 = "medusa_playinfo_isp_playqos_playcode"
  private val tableName3 = "medusa_playinfo_product_playqos_playcode"

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
    ModuleClass.executor(PlayInfoByAreaAndISPStatics, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        sqlContext.udf.register("getProvince",IPLocationDataUtil.getProvince _)
        sqlContext.udf.register("getISP",IPOperatorsUtil.getISPInfo _)
        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        val readPath = ""

        (0 until p.numOfDays).foreach(i => {

          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)

          if(date.equals("20160815")){
            val readPath = s"/log/medusa/parquet/20160814/playqos"
            cal.add(Calendar.DAY_OF_MONTH,-1)
          }
          else{
            val readPath = s"/log/medusa/parquet/$date/playqos"
          }

          val date1 = DateFormatUtils.cnFormat.format(cal.getTime)
          println(readPath)

          sqlContext.read.parquet(readPath).select("date", "userId","ip","productModel",
            "jsonLog")
              .filter(s"date=$date1").registerTempTable("log")

          //(videoSid, day, playcode)
          val rdd = sqlContext.sql("select getProvince(ip),getISP(ip),productModel,userId,jsonLog from log").
            map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4)))

          val tmpRdd = rdd.flatMap(e=>getPlayCode(e._1,e._2,e._3,e._4,e._5)).
            filter(e=>isContained(e._4)).cache()

          val areaInfo = tmpRdd.map(e=>(e._1,e._5)).cache()
          val ispInfo = tmpRdd.map(e=>(e._2,e._5)).cache()
          val productModelInfo = tmpRdd.map(e=>(e._3,e._5)).cache()
          val areaNum = areaInfo.countByKey()
          val areaUser = areaInfo.distinct().countByKey()
          val ispNum = ispInfo.countByKey()
          val ispUser = ispInfo.distinct().countByKey()
          val productNum = productModelInfo.countByKey()
          val productUser = productModelInfo.distinct().countByKey()

          val insertSql1 = s"insert into $tableName1(area,playNum,playUser) values (?,?,?)"
          areaNum.foreach(i=>{
            val key = i._1
            val user = areaUser.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            util.insert(insertSql1,i._1,new JLong(i._2),new JLong(user))
          })

          val insertSql2 = s"insert into $tableName2(isp,playNum,playUser) values (?,?,?)"
          ispNum.foreach(i=>{
            val key = i._1
            val user = ispUser.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            util.insert(insertSql2,i._1,new JLong(i._2),new JLong(user))
          })

          val insertSql3 = s"insert into $tableName3(product,playNum,playUser) values (?,?,?)"
          productNum.foreach(i=>{
            val key = i._1
            val user = productUser.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            util.insert(insertSql3,i._1,new JLong(i._2),new JLong(user))
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
    * @param
    * @param str json字符串
    * @return (videoSid, day, playcode)
    */
  def getPlayCode(area: String,isp:String,productModel:String,userId:String, str: String) = {

    val res = new ListBuffer[(String, String, String,String,String)]()

    try {
      val jsObj = new JSONObject(str)

      val videoSid = jsObj.optString("videoSid")
      res.+= ((area,isp,productModel,videoSid,userId))
      }
    catch {
      case ex: Exception => {
        res.+=(("", "","","",""))
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
      case -1 => -2
      case -2 => -2

    }
  }
}
