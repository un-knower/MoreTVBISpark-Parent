package com.moretv.bi.ProgramViewAndPlayStats

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object KidsCatHousePVVVUV extends BaseClass with DateUtil{
  def main(args: Array[String]): Unit = {
    config.setAppName("KidsCatHousePVVVUV")
    ModuleClass.executor(KidsCatHousePVVVUV,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {


        //calculate log whose type is play
        val path = "/mbi/parquet/{playview,detail}/" + p.startDate + "/part-*"
        val df = sqlContext.read.load(path).persist(StorageLevel.MEMORY_AND_DISK)
        val playRDD = df.filter("logType='playview' and path like '%kids_home%'").select("date", "path", "userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
            map(e => (getKeys(e._1, e._2), e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_play = playRDD.distinct().countByKey()
        val accessNum_play = playRDD.countByKey()

        val detailRDD = df.filter("logType='detail' and path like '%kids_home%'").select("date", "path", "userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
            map(e => (getKeys(e._1, e._2, "detail"), e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_detail = detailRDD.distinct().countByKey()
        val accessNum_detail = detailRDD.countByKey()

        //save date
        val util = new DBOperationUtils("bi")
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from kids_cathouse_pv_vv_uv where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO kids_cathouse_pv_vv_uv(year,month,day,type,parent_code,code,name,user_num,access_num) VALUES(?,?,?,?,?,?,?,?,?)"
        userNum_play.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,x._1._7,new Integer(x._2.toInt),new Integer(accessNum_play(x._1).toInt))
        })

        userNum_detail.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,x._1._7,new Integer(x._2.toInt),new Integer(accessNum_detail(x._1).toInt))
        })

        playRDD.unpersist()
        detailRDD.unpersist()
        df.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def getKeys(date:String, path:String, logType:String = "play")={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    var parentCode = ""
    var code = ""
    var name = ""

    val reg = "(home|thirdparty_\\d{1})-kids_home-(kids_cathouse|kids_songhome-kids_cathouse)-(\\w+)".r
    val pattern = reg findFirstMatchIn path
    pattern match {
      case Some(x) =>
        parentCode = x.group(2)
        code = x.group(3)
        name = getName(parentCode,code)
      case None => null
    }

    (year,month,date,logType,parentCode,code,name)
  }

  //obtain name
  def getName(parentCode:String,code:String)={

    var name = ""
    if(parentCode == "kids_cathouse"){
      val code2NameMap = Map("history"->"动画历史","collect"->"动画收藏","kidssong_collect"->"儿歌收藏")
      name = code2NameMap.getOrElse(code,"")

    }else if(parentCode == "kids_songhome-kids_cathouse"){
      val code2NameMap = Map("history"->"听儿歌-动画历史","collect"->"听儿歌-动画收藏","kidssong_collect"->"听儿歌-儿歌收藏")
      name = code2NameMap.getOrElse(code,"")
    }
    name
  }
}
