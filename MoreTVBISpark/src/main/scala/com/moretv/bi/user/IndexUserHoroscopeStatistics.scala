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
object IndexUserHoroscopeStatistics extends BaseClass with QueryMaxAndMinIDUtil with DateUtil{
  val reg ="log=play-001-playview-[\\w\\.]+_\\d+\\.\\d+\\.\\d+-([a-f0-9]{32}-\\d{8}-\\d*|[a-f0-9]{32}-\\d{8})-.+?-(movie|tv|zongyi|kids|comic|jilu)-([A-Za-z0-9]{12})-".r

  def main(args: Array[String]) {
    config.setAppName("IndexUserHoroscopeStatistics")
    ModuleClass.executor(IndexUserHoroscopeStatistics,args)
  }
  override def execute(args: Array[String]) {

    //查询数据库中最大和最小ID，返回得到数组
    val id = queryID("uid","bbs_ucenter_memberfields","jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true");

    //从数据库中取得出生日期分布
    val userIdRDD = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
    },
      "SELECT uid,IFNULL(birthday,'') FROM `bbs_ucenter_memberfields` WHERE UID >= ? AND UID <= ? and birthday is not null and birthday !='' ",
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
      val genderArray = JsonArray.empty().add(e._2._1).add(e._2._2).add(e._2._3).add(e._2._4).add(e._2._5)
        .add(e._2._6).add(e._2._7).add(e._2._8).add(e._2._9).add(e._2._10).add(e._2._11).add(e._2._12)
      val doc:JsonDocument = bucket.get(e._1)
      if(doc==null){
        val content = JsonObject.empty().put(timeFlage +"_IndexUserHoroscope",genderArray)
        val newDoc = JsonDocument.create(e._1,content)
        bucket.upsert(newDoc)
      }else{
        doc.content().put(timeFlage +"_IndexUserHoroscope",genderArray)
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
  //这个方法是分别统计每个节目的星座比列
  def genderStatistic(ages:Iterable[String])={
    val yesterday = Calendar.getInstance()
    yesterday.add(Calendar.DAY_OF_MONTH, -1)
    val nowYear = yesterday.get(Calendar.YEAR)
    var shui = 0;
    var shuangyu = 0;
    var bai = 0;
    var jin = 0;
    var shuangzi = 0;
    var ju = 0;
    var shi = 0;
    var chu = 0;
    var tianpin = 0;
    var tianxie = 0;
    var she = 0;
    var mo = 0;
    ages.foreach(x =>{
      if(x.contains("月") && x.contains("日")){
        val month = x.substring(x.indexOf('年')+1,x.indexOf('月'));
        val day   = x.substring(x.indexOf('月')+1,x.indexOf('日'));
        try{
          val horoscop = getHoroscopeName(month.toInt,day.toInt);
          horoscop match {
            case "水瓶座" => shui=shui+1
            case "双鱼座" => shuangyu=shuangyu+1
            case "白羊座" => bai=bai+1
            case "金牛座" => jin=jin+1
            case "双子座" => shuangzi=shuangzi+1
            case "巨蟹座" => ju=ju+1
            case "狮子座" => shi=shi+1
            case "处女座" => chu=chu+1
            case "天秤座" => tianpin=tianpin+1
            case "天蝎座" => tianxie=tianxie+1
            case "射手座" => she=she+1
            case "摩羯座" => mo=mo+1
          }
        }catch {
          case e:Exception => {null}
        }
      }else null
    })
    (shui,shuangyu,bai,jin,shuangzi,ju,shi,chu,tianpin,tianxie,she,mo)
  }
  //获取星座名
  def getHoroscopeName(month:Int , day:Int):String={
    var star = "";
    if (month == 1 && day >= 20 || month == 2 && day <= 18) {
      star = "水瓶座";
    }
    if (month == 2 && day >= 19 || month == 3 && day <= 20) {
      star = "双鱼座";
    }
    if (month == 3 && day >= 21 || month == 4 && day <= 19) {
      star = "白羊座";
    }
    if (month == 4 && day >= 20 || month == 5 && day <= 20) {
      star = "金牛座";
    }
    if (month == 5 && day >= 21 || month == 6 && day <= 21) {
      star = "双子座";
    }
    if (month == 6 && day >= 22 || month == 7 && day <= 22) {
      star = "巨蟹座";
    }
    if (month == 7 && day >= 23 || month == 8 && day <= 22) {
      star = "狮子座";
    }
    if (month == 8 && day >= 23 || month == 9 && day <= 22) {
      star = "处女座";
    }
    if (month == 9 && day >= 23 || month == 10 && day <= 22) {
      star = "天秤座";
    }
    if (month == 10 && day >= 23 || month == 11 && day <= 21) {
      star = "天蝎座";
    }
    if (month == 11 && day >= 22 || month == 12 && day <= 21) {
      star = "射手座";
    }
    if (month == 12 && day >= 22 || month == 1 && day <= 19) {
      star = "摩羯座";
    }
    star;
  }

}
