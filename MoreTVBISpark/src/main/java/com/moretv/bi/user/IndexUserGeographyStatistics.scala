package com.moretv.bi.user

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DateUtil, QueryMaxAndMinIDUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Will on 2015/4/18.
 */
object IndexUserGeographyStatistics extends BaseClass with QueryMaxAndMinIDUtil with DateUtil{
  val reg ="log=play-001-playview-[\\w\\.]+_\\d+\\.\\d+\\.\\d+-([a-f0-9]{32}-\\d{8}-\\d*|[a-f0-9]{32}-\\d{8})-.+?-(movie|tv|zongyi|kids|comic|jilu)-([A-Za-z0-9]{12})-".r

  def main(args: Array[String]) {
    config.setAppName("IndexUserGeographyStatistics")
    ModuleClass.executor(IndexUserGeographyStatistics,args)
  }
  override def execute(args: Array[String]) {

    //查询数据库中最大和最小ID，返回得到数组
    val id = queryID("uid","bbs_ucenter_memberfields","jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true");

    //从数据库中取得性别分布
    val userIdRDD = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
    },
      "SELECT uid,IFNULL(area,'') FROM `bbs_ucenter_memberfields` WHERE UID >= ? AND UID <= ? and area is not null and area !='' ",
      id(1), id(0), 10,
      r=>(r.getInt(1),r.getString(2))).filter(_!=null)

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
    val resultRDD = programRDD.join(userIdRDD).map(e=>(e._2._1,e._2._2)).groupByKey().map(e => (e._1,genderStatistic(e._2))).filter(e=>e._2!=null).collect()

    //连接couchbase，并存储数据
    val cluster = CouchbaseCluster.create("10.10.7.2,10.10.8.2");
    val bucket = cluster.openBucket("ProgramIndex","mlw321@moretv",3,TimeUnit.MINUTES);
    resultRDD.foreach(e =>{
      val genderArray = JsonArray.empty()
      e._2.foreach(x=>{
        genderArray.add(x)
      })
      val doc:JsonDocument = bucket.get(e._1)
      if(doc == null){
        val content = JsonObject.empty().put(timeFlage+"_IndexUserGeography",genderArray)
        val newDoc = JsonDocument.create(e._1,content)
        bucket.upsert(newDoc)
      }else{
        doc.content.put(timeFlage+"_IndexUserGeography",genderArray)
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

  //这个方法是分别统计每个节目用户地区分布
  def genderStatistic(areas:Iterable[String])={
    var beijing = 0
    var tianjin = 0
    var shanghai = 0
    var chongqing = 0
    var hebei = 0
    var henan = 0
    var yunnan = 0
    var liaoning = 0
    var heilongjiang = 0
    var hunan = 0
    var anhui = 0
    var shandong = 0
    var xinjiang = 0
    var jiangsu = 0
    var zhejiang = 0
    var jiangxi = 0
    var hubei = 0
    var guangxi = 0
    var gansu = 0
    var shanxi = 0
    var neimeng = 0
    var shaxi = 0
    var jiling = 0
    var fujian = 0
    var guizhou = 0
    var guangdong = 0
    var qinghai = 0
    var xizang = 0
    var sichuan = 0
    var ningxia = 0
    var hainan = 0
    var taiwan = 0
    var xianggan = 0
    var aomen = 0
    //处理数据
    areas.foreach(x =>{
      val temp =  x.split(",")
      if (temp.length < 0 || temp.length == 0){
        null
      }else if (temp.length == 1 || temp.length  > 1){
        temp(0) match {
          case "北京" =>beijing=beijing+1
          case "天津" =>tianjin=tianjin+1
          case "上海" =>shanghai=shanghai+1
          case "重庆" =>chongqing=chongqing+1
          case "河北" =>hebei=hebei+1
          case "河南" =>henan=henan+1
          case "云南" =>yunnan=yunnan+1
          case "辽宁" =>liaoning=liaoning+1
          case "黑龙江" =>heilongjiang=heilongjiang+1
          case "湖南" =>hunan=hunan+1
          case "安徽" =>anhui=anhui+1
          case "山东" =>shandong=shandong+1
          case "新疆" =>xinjiang=xinjiang+1
          case "江苏" =>jiangsu=jiangsu+1
          case "浙江" =>zhejiang=zhejiang+1
          case "江西" =>jiangxi=jiangxi+1
          case "湖北" =>hubei=hubei+1
          case "广西" =>guangxi=guangxi+1
          case "甘肃" =>gansu=gansu+1
          case "山西" =>shanxi=shanxi+1
          case "内蒙古" =>neimeng=neimeng+1
          case "陕西" =>shaxi=shaxi+1
          case "吉林" =>jiling=jiling+1
          case "福建" =>fujian=fujian+1
          case "贵州" =>guizhou=guizhou+1
          case "广东" =>guangdong=guangdong+1
          case "青海" =>qinghai=qinghai+1
          case "西藏" =>xizang=xizang+1
          case "四川" =>sichuan=sichuan+1
          case "宁夏" =>ningxia=ningxia+1
          case "海南" =>hainan=hainan+1
          case "台湾" =>taiwan=taiwan+1
          case "香港" =>xianggan=xianggan+1
          case "澳门" =>aomen=aomen+1
        }
      }
    })
    List(beijing,tianjin,shanghai,chongqing,hebei,henan,yunnan,liaoning,heilongjiang,hunan,anhui,shandong,xinjiang,jiangsu,zhejiang,jiangxi,hubei,guangxi,gansu,shanxi,neimeng,shaxi,jiling,fujian,guizhou,guangdong,qinghai,xizang,sichuan,ningxia,hainan,taiwan,xianggan,aomen)
  }

}
