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
object IndexDianBoTop500Statistics extends BaseClass with QueryMaxAndMinIDUtil with DateUtil{
  val reg ="log=play-001-playview-[\\w\\.]+_\\d+\\.\\d+\\.\\d+-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-.+?-(movie|tv|zongyi|kids|comic|jilu)-([A-Za-z0-9]{12})-".r

  def main(args: Array[String]) {
    config.setAppName("IndexDianBoTop500Statistics")
    ModuleClass.executor(IndexDianBoTop500Statistics,args)
  }
  override def execute(args: Array[String]) {

    //查询数据库中最大和最小ID，返回得到数组
    val id = queryID("id","mtv_program","jdbc:mysql://10.10.2.19:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true");

    //从数据库中取得年龄分布
    val doubanTagRDD = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.19:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
    },
      "SELECT sid,IFNULL(douban_tags,'') FROM `mtv_program` WHERE ID >= ? AND ID <= ? and douban_tags is not null and douban_tags !='' ",
      id(1), id(0), 10,
      r=>(r.getString(1),r.getString(2))).filter(_!=null).filter(e => e._2 != "")

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
    val programRDD = sc.textFile(inputFile).map(matchLog).filter(_!=null).reduceByKey((x,y)=>x+y).sortBy(-_._2).take(500)

    //合并两个RDD
    val doubanResultRDD = sc.parallelize(programRDD).join(doubanTagRDD).map(_._2._2).flatMap(e => e.split("\\|")).map((_,1)).reduceByKey((x,y)=>x+y).sortBy(-_._2).collect()

    //连接couchbase，并存储数据
    val cluster = CouchbaseCluster.create("10.10.7.2,10.10.8.2");
    val bucket = cluster.openBucket("MoreTvTop","mlw321@moretv",3,TimeUnit.MINUTES);
    val tagsArray = JsonArray.empty()
    val dateArray = JsonArray.empty()
    doubanResultRDD.foreach(e =>{
      tagsArray.add(e._1);
      dateArray.add(e._2);
    })
    val content = JsonObject.empty().put("tag",tagsArray).put("date",dateArray)
    val doc = JsonDocument.create(timeFlage+"_IndexDianBoTagTop500",content)
    bucket.upsert(doc)
    cluster.disconnect()

  }
  //处理日志返回uid与节目的键值对
  def matchLog(log:String) = {
    val pattern = reg findFirstMatchIn log
    pattern match {
      case Some(x) =>
        val userId = x.group(1)
        val programId = x.group(3)
        val uid = userId.substring(0,32)
        if(uid.length < 32){
          null
        }else{
          (programId,1)
        }
      case None => null
    }
  }

}
