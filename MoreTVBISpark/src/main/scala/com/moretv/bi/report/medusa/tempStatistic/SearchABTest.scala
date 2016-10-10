/**
package com.moretv.bi.report.medusa.tempStatistic

import java.io.{FileOutputStream, OutputStreamWriter, File}
import java.lang.{Long=>JLong}
import cn.whaley.recommend.abtest.ABTestOne
import cn.whaley.recommend.count.CountABTest
import cn.whaley.sdk.utils.TransformUDF
import com.moretv.bi.util.{DBOperationUtils, ParamsParseUtil, SparkSetting}
import com.sun.deploy.util.ParameterUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/8/31.
 */
object SearchABTest extends SparkSetting{
  lazy val savepath ="/home/spark/xiajun/medusaSearch/"
  lazy val dateInfo = "20160902~20160908"
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    val sc = new SparkContext(config)
    val sqlContext = SQLContext.getOrCreate(sc)
    TransformUDF.registerUDF(sqlContext)
    val util = new DBOperationUtils("medusa")
    val configFile = new File("/script/bi/medusa/test/shell/configFile/search.xml")
    val day = "201609{02,03,04,05,06,07,08}"
    val abTest = new ABTestOne(configFile)
    val countAbTest = new CountABTest(abTest)

    // Getting the enter search info
    val inputSearchEnterDir = s"/log/medusa/parquet/$day/search-tabview"
    val searchEnterRdd = sqlContext.read.parquet(inputSearchEnterDir).repartition(20).
      select("userId","apkVersion","accountId").filter("apkVersion = '3.0.9' and userId <> ''").
      selectExpr("userId").rdd.map(e=>(e.getString(0))).map(e=>(abTest.getGroup(e),e))
    val groupUserCount = searchEnterRdd.distinct().countByKey()
    val groupNumCount = searchEnterRdd.countByKey()
    // Getting the play info
    val inputPlayDir = s"/log/medusa/parquet/$day/play"
    val playDF = sqlContext.read.parquet(inputPlayDir).repartition(20).select("userId","apkVersion","pathMain","duration","event","accountId").
      filter("apkVersion = '3.0.9' and userId <> '' and event='startplay'").filter("pathMain like '%search%'")
    val playCount = countAbTest.countPlay(playDF,groupUserCount)     // 计算置信度
    (0 until playCount._1.length).foreach(i=>{
      val groupName = playCount._1(i).name
      val groupResult = playCount._1(i).result
      println("GroupName: "+groupName+" "+groupResult.toString)
    })
    val info = playCount._2(0)
    println("Info: "+info.toString())

    //    val playRdd = playDF.selectExpr("userId").rdd.map(e=>e.getString(0)).repartition(24).
//                  map(e=>(abTest.getGroup(e),e)).repartition(28)
//    val playNum = playRdd.countByKey()
//    val playUser = playRdd.distinct().countByKey()

    // Getting the detail interview info
    val inputDetailDir = s"/log/medusa/parquet/$day/detail"
    val detailDF = sqlContext.read.parquet(inputDetailDir).repartition(20).select("userId","apkVersion","pathMain","accountId").
      filter("apkVersion = '3.0.9' and userId <> ''").filter("pathMain like '%search%'")
    val detailCount = countAbTest.countDetail(detailDF,groupUserCount)
    (0 until detailCount._1.length).foreach(i=>{
      val groupName = detailCount._1(i).name
      val groupResult = detailCount._1(i).result
      println("GroupName: "+groupName+" "+groupResult.toString)
    })
    val info1 = playCount._2(0)
    println("Info: "+info1.toString())



    //    val detailRdd = detailDF.selectExpr("userId").rdd.map(e=>(e.getString(0))).repartition(24).
//                    map(e=>(abTest.getGroup(e),e)).repartition(28)
//    val detailNum = detailRdd.countByKey()
//    val detailUser = detailRdd.distinct().countByKey()

    // 将播放、详情页浏览和进入搜索的数据插入到数据库
//    val insertSql = "insert into search_ab_test_play(day,groupName,enter_num,enter_user,detail_num,detail_user,play_num,play_user) " +
//      "values(?,?,?,?,?,?,?,?)"
//    groupNumCount.foreach(i=>{
//      val key = i._1
//      val groupUserInfo = groupUserCount.get(key) match {
//        case Some(p) => p
//        case None => 0L
//      }
//      val playNumInfo = playNum.get(key) match {
//        case Some(p) => p
//        case None => 0L
//      }
//      val playUserInfo = playUser.get(key) match {
//        case Some(p) => p
//        case None => 0L
//      }
//      val detailNumInfo = detailNum.get(key) match {
//        case Some(p) => p
//        case None => 0L
//      }
//      val detailUserInfo = detailUser.get(key) match {
//        case Some(p) => p
//        case None => 0L
//      }
//      util.insert(insertSql,"2016-09-01~2016-09-07",i._1,new JLong(i._2),new JLong(groupUserInfo),new JLong(detailNumInfo),
//        new JLong(detailUserInfo),new JLong(playNumInfo),new JLong(playUserInfo))
//    })


    // 将评估的数据写入文件中
//    val dir = s"$savepath$dateInfo"
//    val osw = new OutputStreamWriter(new FileOutputStream(dir))
//
//    osw.write("play\n")
//    playCount._1.map(x=> (x.name+"\t"+x.result.toString))
//      .foreach(x=>osw.write(x+"\n"))
//    playCount._2.foreach(x=>osw.write(x._1+"\t"+x._2+"\t"+x._3.toString+"\n"))
//    osw.write("=========================\ndetail\n")
//    detailCount._1.map(x=>(x.name+"\t"+x.result.toString)).
//      foreach(x=>osw.write(x+"\n"))
//    detailCount._2.foreach(x=>osw.write(x._1+"\t"+x._2+"\t"+x._3.toString+"\n"))
//
//    osw.write("group\tuv\tvv\t平均播放时长\t平均每人时长\n")
//
//    playCount._1.foreach(x=>{
//      osw.write(s"${x.name}\t${x.result.uvRatio}\t${x.result.vvRatio}\t${x.result.averageWatchDuration}\t${x.result.averageUserDuration}\n")
//    })
//    osw.write("group\tuv\tpv\n")
//    detailCount._1.foreach(x=>{
//      osw.write(s"${x.name}\t${x.result.uvRatio}\t${x.result.pvRatio}\n")
//    })
//
//    osw.close()

  }

}
*/