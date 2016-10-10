package com.moretv.bi.user

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.moretv.bi.util.FileUtils._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DateUtil, QueryMaxAndMinIDUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Will on 2015/4/18.
 */
object IndexUserGenderStatistics extends BaseClass with QueryMaxAndMinIDUtil with DateUtil{
  val reg ="log=play-001-playview-[\\w\\.]+_\\d+\\.\\d+\\.\\d+-([a-f0-9]{32}-\\d{8}-\\d*|[a-f0-9]{32}-\\d{8})-.+?-(movie|tv|zongyi|kids|comic|jilu)-([A-Za-z0-9]{12})-".r

  def main(args: Array[String]) {
    config.setAppName("IndexUserGenderStatistics")
    ModuleClass.executor(IndexUserGenderStatistics,args)
  }
  override def execute(args: Array[String]) {

    //查询数据库中最大和最小ID，返回得到数组
    val id = queryID("uid","bbs_ucenter_memberfields","jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true");

    //从数据库中取得性别分布
    val userIdRDD = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
    },
      "SELECT uid,gender FROM `bbs_ucenter_memberfields` WHERE UID >= ? AND UID <= ? and gender is not null and gender !='' ",
      id(1), id(0), 10,
      r=>(r.getInt(1),r.getString(2))).filter(_!=null).filter(e => (e._2 == "f" || e._2 == "m"))

    //根据不同请求拼接不同的输入路径：上一周，上一个月，上三个月
    var timeFlage = ""
    var inputFile = "/mbi/LogApart/{"
    val timeRegion = getDateRegion(args(0),args(1))
    timeRegion.foreach(x =>{
      inputFile = inputFile + x + ","
    })
    inputFile = inputFile.substring(0,inputFile.length-1) +"}/play/*"
    if(args(0) == "week"){
      timeFlage = timeRegion(timeRegion.length-1) +"-"+ timeRegion(0)
    }else if(args(0) == "oneMonth"){
      timeFlage = timeRegion(0).substring(0,timeRegion(0).length-1)
    }else if(args(0) == "threeMonth"){
      timeFlage = timeRegion(2).substring(0,timeRegion(2).length-1) +"-"+ timeRegion(0).substring(0,timeRegion(0).length-1)
    }
    //从日志中读取用户和节目的对应数据
    val programRDD = sc.textFile(inputFile).map(matchLog).filter(_!=null).distinct()

    //合并两个RDD
    val resultRDD = programRDD.join(userIdRDD).map(e=>(e._2._1,e._2._2)).groupByKey().map(e => (e._1,genderStatistic(e._2))).collect()

    //连接couchbase，并存储数据
    val cluster = CouchbaseCluster.create("10.10.7.2,10.10.8.2");
    val bucket = cluster.openBucket("ProgramIndex","mlw321@moretv",3,TimeUnit.MINUTES);
    resultRDD.foreach(e =>{
      val genderArray = JsonArray.empty().add(e._2._1).add(e._2._2)
      val doc:JsonDocument = bucket.get(e._1)
      if(doc == null){
        val content = JsonObject.empty().put(timeFlage+"_IndexUserGender",genderArray)
        val newDoc = JsonDocument.create(e._1,content)
        bucket.upsert(newDoc)
      }else{
        doc.content().put(timeFlage+"_IndexUserGender",genderArray)
        bucket.upsert(doc)
      }
    })
    cluster.disconnect()

  }
  //处理日志返回uid与节目的键值对
  def matchLog(log:String) = {
    val pattern = reg findFirstMatchIn log
    pattern match {
      case Some(x) =>
        val userId = x.group(1)
        val programId = x.group(3)
        val accountId = userId.split("-")(1)
        val uid = accountToUid(accountId.toInt)
        if(uid == -1){
          null
        }else{
          (uid,programId)
        }
      case None => null
    }
  }
  //这个方法的作用是将accountId转化成UserId
  def accountToUid(accountId:Int):Int={
    val total:Long=90000000
    val base:Long=10000000
    val p_reverse:Long=62455379

    if (accountId <= base || accountId>=total+base)
      return -1
    val a=accountId - base
      (a*p_reverse%total).toInt
  }
  //这个方法是分别统计每个节目的男女比列
  def genderStatistic(genders:Iterable[String])={
    var female = 0;
    var male = 0;
    genders.foreach(x =>{
      if(x=="f")
        female=female+1
      else if (x == "m")
        male=male+1
    })
    (female,male)
  }

}
