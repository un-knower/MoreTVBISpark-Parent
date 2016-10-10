package com.moretv.bi.xutong

import java.lang.{Double => JDouble}
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xutong on 15/12/24.
 */
object RecommendAssessDetailUVVV extends BaseClass{
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("RecommendAssessDetailUVVV").
      setMaster("spark://10.10.2.14:7077").
      set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max","50").
      //      set("spark.driver.memory", "5g").
      set("spark.scheduler.mode", "FAIR")
    ModuleClass.executor(RecommendAssessDetailUVVV,args)
  }
  override def execute(args: Array[String]) = {


    ParamsParseUtil.parse(args) match {
      case Some(p) => {//calculate log whose type is play
        val util = new DBOperationUtils("bi")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val dateTime = DateFormatUtils.readFormat.format(cal.getTime)
          val path = "/mbi/parquet/playview/" + dateTime + "/part-*"

          cal.add(Calendar.DAY_OF_MONTH, -1)
          val dateFirstTime = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(dateFirstTime)

          val collectMap = Map(
            "zhuanti"->"subject",
            "otherswatch"->"otherswatch",
            "otherwatch_map"->"otherwatch_map",
            "otherwatch_nearby"->"otherwatch_nearby",
            "guess"->"guess",
            "hotrecommend"->"handcraft",
            "similar"->"similar",
            "peoplealsolike"->"peoplealsolike",
            "interestcommend"->"interestcommend"
          )

          val df = sqlContext.read.load(path).persist(StorageLevel.MEMORY_AND_DISK)
          val playRDD = df.select("path", "userId", "duration").map(e => (e.getString(0), e.getString(1), e.getInt(2))).
            filter(e =>e._3>0 && e._3<72000).map(e => (getPath(e._1,e._2), e._2, e._3)).filter(_._1 != "").filter(x => collectMap.keySet.contains(x._1)).
            persist(StorageLevel.MEMORY_AND_DISK)

          val userNum_play = playRDD.map(e => (e._1, e._2)).distinct().groupByKey().map(e=>(e._1,e._2.size)).collect()
          val accessNum_play = playRDD.map(e => (e._1, e._2)).groupByKey().map(e=>(e._1,e._2.size)).collect().toMap

          val time_sum = playRDD.map(e => (e._1, new JDouble(e._3))).reduceByKey(_+_).collect().toMap
          //save date

          //delete old data
          if (p.deleteOld) {
            val oldSql = s"delete from recommend_evaluation where date = ?"
            util.delete(oldSql,day)
          }
          //insert new data

          val sql = "INSERT INTO recommend_evaluation(date,type,uv,vv,duration) VALUES(?,?,?,?,?)"
          userNum_play.foreach(x => {
            util.insert(sql, day, collectMap(x._1),new Integer(x._2), new Integer(accessNum_play(x._1)),
              new JDouble(time_sum(x._1)))
          })
          playRDD.unpersist()
          df.unpersist()

        })
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getPath(path: String,userId: String)= {
    val tmp = path.split('-')
    val len = tmp.length
    if (tmp.isEmpty)
      ""
    else {
      if (tmp(len - 1) == "similar" || tmp(len - 1) == "peoplealsolike" || tmp(len - 1) == "otherwatch_nearby" || tmp(len - 1) == "otherwatch_map" || tmp(len - 1) == "otherswatch")
        tmp(len-1)
      else if(len == 3 && (tmp(len - 1) == "tv" || tmp(len - 1) == "movie"))
        "guess"
      else {
        val reg = "(movie|tv|zongyi|comic|jilu|hot)([0-9]+)".r
        if(reg.findFirstMatchIn(tmp(len-1)) != None)
          "zhuanti"
        else if(len == 4 && tmp(1) == "hotrecommend"){
          if(generateCluster(userId) && (tmp(2) == "12" || tmp(2) == "13" || tmp(2) == "14"))
            "interestcommend"
          else tmp(1)
        }
        else
          ""
      }
    }
  }
  /**
   * this method is to divide users to different group for AB testing.
   * @param userId : user id
   * @return : if the group number is between 1 and 500, true. else false.
   */
  def generateCluster(userId : String) : Boolean={
    val a = userId.hashCode() & Integer.MAX_VALUE
    val m = a>>2 + Math.pow(a, 2).toInt>>4
    if((m%1000+1)>=1 && (m%1000+1)<=500)
      true
    else false
  }
}