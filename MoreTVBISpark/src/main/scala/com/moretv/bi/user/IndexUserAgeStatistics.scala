package com.moretv.bi.user

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateUtil, QueryMaxAndMinIDUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Will on 2015/4/18.
 */
object IndexUserAgeStatistics extends BaseClass with QueryMaxAndMinIDUtil with DateUtil{
  val reg ="log=play-001-playview-[\\w\\.]+_\\d+\\.\\d+\\.\\d+-([a-f0-9]{32}-\\d{8}-\\d*|[a-f0-9]{32}-\\d{8})-.+?-(movie|tv|zongyi|kids|comic|jilu)-([A-Za-z0-9]{12})-".r

  def main(args: Array[String]) {
    config.setAppName("IndexUserAgeStatistics")
    ModuleClass.executor(IndexUserAgeStatistics,args)
  }
  override def execute(args: Array[String]) {

    //查询数据库中最大和最小ID，返回得到数组
    val id = queryID("uid","bbs_ucenter_memberfields","jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true");

    //从数据库中取得年龄分布
    val userIdRDD = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
    },
      "SELECT uid,IFNULL(LEFT(birthday,4),'') FROM `bbs_ucenter_memberfields` WHERE UID >= ? AND UID <= ? and birthday is not null and birthday !='' ",
      id(1), id(0), 10,
      r=>(r.getInt(1),r.getString(2))).filter(_!=null).filter(e => e._2 != "")

    //根据不同请求拼接不同的输入路径：上一周，上一个月，上三个月
    var timeFlage = ""
    var inputFile = "/mbi/LogApart/{"
    val timeRegion = getDateRegion(args(0),args(1))
    timeRegion.foreach(x =>{
      inputFile = inputFile + x + ","
    })
    inputFile = inputFile.substring(0,inputFile.length-1) +"}/play/*"
    if(args(0) == "week"){
      timeFlage = timeRegion(timeRegion.length-1) + "-" + timeRegion(0)
    }else if(args(0) == "oneMonth"){
      timeFlage = timeRegion(0).substring(0,timeRegion(0).length-1)
    }else if(args(0) == "threeMonth"){
      timeFlage = timeRegion(2).substring(0,timeRegion(2).length-1) + "-" + timeRegion(0).substring(0,timeRegion(0).length-1)
    }
    //从日志中读取用户和节目的对应数据
    val programRDD = sc.textFile(inputFile).map(matchLog).filter(_!=null).distinct()

    //合并两个RDD
    val resultRDD = programRDD.join(userIdRDD).map(e=>(e._2._1,e._2._2)).groupByKey().map(e => (e._1,genderStatistic(e._2))).filter(e=>e._2!=null).collect()

    //连接couchbase，并存储数据
    val cluster = CouchbaseCluster.create("10.10.7.2,10.10.8.2");
    val bucket = cluster.openBucket("ProgramIndex","mlw321@moretv",3,TimeUnit.MINUTES);
    resultRDD.foreach(e =>{
      val doc:JsonDocument = bucket.get(e._1)
      val genderArray = JsonArray.empty().add(e._2._1).add(e._2._2).add(e._2._3).add(e._2._4).add(e._2._5)
      if(doc == null){
        val content = JsonObject.empty().put(timeFlage+"_IndexUserAge",genderArray)
        val newDoc = JsonDocument.create(e._1,content)
        bucket.upsert(newDoc)
      }else{
        doc.content.put(timeFlage+"_IndexUserAge",genderArray)
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
    return (a*p_reverse%total).toInt
  }
  //这个方法是分别统计每个节目的各个年龄段的人数
  def genderStatistic(ages:Iterable[String])={
    val yesterday = Calendar.getInstance()
    yesterday.add(Calendar.DAY_OF_MONTH, -1)
    val nowYear = yesterday.get(Calendar.YEAR)
    var first = 0;
    var second = 0;
    var third = 0;
    var four = 0;
    var five = 0;
    ages.foreach(x =>{
      val age = nowYear - Integer.parseInt(x)
      if(age>0 && age <=19){
        first=first+1
      }else if(age>19 && age <=29){
        second=second+1
      }else if(age>29 && age <=39){
        third=third+1
      }else if(age>39 && age <=49){
        four=four+1
      }else if(age>49){
        five=five+1
      }else null
    })
    (first,second,third,four,five)
  }

}
