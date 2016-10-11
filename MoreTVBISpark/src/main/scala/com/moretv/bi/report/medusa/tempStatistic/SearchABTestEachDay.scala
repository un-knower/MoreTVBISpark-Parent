/**
package com.moretv.bi.report.medusa.tempStatistic

import java.io.File
import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.recommend.abtest.ABTestOne
import cn.whaley.recommend.count.CountABTest
import cn.whaley.sdk.utils.TransformUDF
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, DBOperationUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by xiajun on 2016/8/31.
 */
object SearchABTestEachDay extends SparkSetting{
  lazy val savepath ="/home/spark/xiajun/medusaSearch/"
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    val sc = new SparkContext(config)
    val sqlContext = SQLContext.getOrCreate(sc)
    TransformUDF.registerUDF(sqlContext)
    val util = new DBOperationUtils("medusa")
    val configFile = new File("/script/bi/medusa/test/shell/configFile/search.xml")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=> {
          val dateTime = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(dateTime, -1)
          val inputSearchEnterDir = s"/log/medusa/parquet/$dateTime/homeaccess"
          val abTest = new ABTestOne(configFile)
          val countAbTest = new CountABTest(abTest)
          val searchEnterRdd = sqlContext.read.parquet(inputSearchEnterDir).repartition(20).
            select("userId","apkVersion","accessArea","accessLocation","accountId").filter("apkVersion = '3.0.9' and " +
            "userId <> '' and accessArea='navi' and accessLocation='search'").selectExpr("userId").
            rdd.map(e=>(e.getString(0))).map(e=>(abTest.getGroup(e),e))
          val groupUserCount = searchEnterRdd.distinct().countByKey()
          val groupNumCount = searchEnterRdd.countByKey()
          // Getting the play info
          val inputPlayDir = s"/log/medusa/parquet/$dateTime/play"
          val playDF = sqlContext.read.parquet(inputPlayDir).repartition(20).
            select("userId","apkVersion","pathMain","duration","event","accountId").
            filter("apkVersion = '3.0.9' and userId <> '' and event='startplay'").
            filter("pathMain like 'home-search*%'")

          val playRdd = playDF.selectExpr("userId").rdd.map(e=>e.getString(0)).repartition(24).
                        map(e=>(abTest.getGroup(e),e)).repartition(28)
          val playNum = playRdd.countByKey()
          val playUser = playRdd.distinct().countByKey()

          // Getting the detail interview info
          val inputDetailDir = s"/log/medusa/parquet/$dateTime/detail"
          val detailDF = sqlContext.read.parquet(inputDetailDir).repartition(20).
            select("userId","apkVersion","pathMain","event","accountId").
            filter("apkVersion = '3.0.9' and userId <> '' and event = 'view'").
            filter("pathMain like 'home-search*%'")

          val detailRdd = detailDF.selectExpr("userId").rdd.map(e=>(e.getString(0))).repartition(24).
                          map(e=>(abTest.getGroup(e),e)).repartition(28)
          val detailNum = detailRdd.countByKey()
          val detailUser = detailRdd.distinct().countByKey()

          val insertSql = "insert into search_ab_test_play(day,groupName,enter_num,enter_user," +
            "detail_num,detail_user,play_num,play_user) values(?,?,?,?,?,?,?,?)"
          groupNumCount.foreach(i=>{
            val key = i._1
            val groupUserInfo = groupUserCount.get(key) match {
              case Some(p) => p
              case None => 0L
            }
            val playNumInfo = playNum.get(key) match {
              case Some(p) => p
              case None => 0L
            }
            val playUserInfo = playUser.get(key) match {
              case Some(p) => p
              case None => 0L
            }
            val detailNumInfo = detailNum.get(key) match {
              case Some(p) => p
              case None => 0L
            }
            val detailUserInfo = detailUser.get(key) match {
              case Some(p) => p
              case None => 0L
            }
            util.insert(insertSql,day,i._1,new JLong(i._2),new JLong(groupUserInfo),new JLong(detailNumInfo),
              new JLong(detailUserInfo),new JLong(playNumInfo),new JLong(playUserInfo))
          })
          // 计算播放的置信度
          val playCount = countAbTest.countPlay(playDF,groupUserCount)
          (0 until playCount._1.length).foreach(i=>{
            val groupName = playCount._1(i).name
            val groupResult = playCount._1(i).result
            println("GroupName: "+groupName+" "+groupResult.toString)
          })
          val info = playCount._2(0)
          println("Info: "+info.toString())
          //计算详情页的置信度
          val detailCount = countAbTest.countDetail(detailDF,groupUserCount)
          (0 until detailCount._1.length).foreach(i=>{
            val groupName = detailCount._1(i).name
            val groupResult = detailCount._1(i).result
            println("GroupName: "+groupName+" "+groupResult.toString)
          })
          val info1 = detailCount._2(0)
          println("Info: "+info1.toString())

          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
    }

  }

}
*/