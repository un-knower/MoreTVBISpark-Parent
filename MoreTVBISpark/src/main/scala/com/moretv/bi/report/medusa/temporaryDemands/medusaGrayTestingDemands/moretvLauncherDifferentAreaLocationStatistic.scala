package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.util.udf.LauncherAccessAreaParser
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel


/**
 * Created by xiajun on 2016/5/12.
 *
 * 统计Moretv首页的点击情况
 */
object moretvLauncherDifferentAreaLocationStatistic extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        sqlContext.udf.register("launcherAccessAreaParser",LauncherAccessAreaParser.launcherAccessAreaParser _)
        sqlContext.udf.register("moretvLauncherAccessLocationParser",LauncherAccessAreaParser.moretvLauncherAccessLocationParser _)
        val util = new DBOperationUtils("bi")
        val logType = "homeaccess"
        val fileDir = "/mbi/parquet/"

        /**
         * 统计各个区域中不同位置在不同版本和不同产品中的点击人数与次数！
         */
        val sqlSpark = "select apkVersion,productModel,accessArea,launcherAccessAreaParser(accessArea,'moretv')," +
          "accessLocation,moretvLauncherAccessLocationParser(accessArea,accessLocation),count(userId), count(distinct " +
          "userId) from log_data group by apkVersion,productModel,accessArea,accessLocation," +
          "launcherAccessAreaParser(accessArea,'moretv'),moretvLauncherAccessLocationParser(accessArea,accessLocation)"

        val sqlDifferentLocationSpark = "select apkVersion,accessArea,launcherAccessAreaParser(accessArea,'moretv')," +
          "accessLocation,moretvLauncherAccessLocationParser(accessArea,accessLocation),count(userId), count(distinct " +
          "userId) from log_data group by apkVersion,accessArea,moretvLauncherAccessLocationParser(accessArea,accessLocation),accessLocation,launcherAccessAreaParser(accessArea," +
          "'moretv')"
        val sqlSumSpark = "select apkVersion,accessArea,launcherAccessAreaParser(accessArea,'moretv'),count(userId), count" +
          "(distinct userId) from log_data group by apkVersion,accessArea," +
          "launcherAccessAreaParser(accessArea,'moretv')"
        val sqlInsert = "insert into moretv_launcher_area_accessLocation_info_by_product_version(day,apkVersion,productModel," +
          "accessArea,accessAreaName,accessLocation,accessLocationName,click_num,click_user) values (?,?,?,?,?,?,?,?,?)"
        val sqlDelete = "delete from moretv_launcher_area_accessLocation_info_by_product_version where day = ?"
        val startDate = p.startDate

        val calDir = Calendar.getInstance()
        calDir.setTime(DateFormatUtils.readFormat.parse(startDate))

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val day = DateFormatUtils.readFormat.format(cal.getTime)
          val date = DateFormatUtils.toDateCN(day,-1)

          // 获取每天点击的人数与次数
          // 从parquet中获取数据
          val logData = sqlContext.read.parquet(s"$fileDir$logType/$day").persist(StorageLevel.DISK_ONLY)
          logData.select("apkVersion","productModel","accessArea","accessLocation","userId").
            registerTempTable("log_data")
          val clickInfoDf = sqlContext.sql(sqlSpark)
          val clickInfoRdd = clickInfoDf.map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),
            e.getString(5),e.getLong(6),e.getLong(7)))

          // 删除数据
          if(p.deleteOld){
            util.delete(sqlDelete,date)
          }

          // 插入数据
          clickInfoRdd.collect.foreach(i=>{
            util.insert(sqlInsert,date,i._1,i._2,i._3,i._4,i._5,i._6,new JLong(i._7), new JLong(i._8))
          })


          val eachLocationByVersion = sqlContext.sql(sqlDifferentLocationSpark).map(e=>(e.getString(0),e.getString(1),e
            .getString(2),e.getString(3),e.getString(4),e.getLong(5),e.getLong(6)))

          eachLocationByVersion.collect.foreach(i=>{
            util.insert(sqlInsert,date,i._1,"All",i._2,i._3,i._4,i._5,new JLong(i._6), new JLong(i._7))
          })

          val sumInfoByVersion = sqlContext.sql(sqlSumSpark).map(e=>(e.getString(0),e.getString(1),e.getString(2),e
            .getLong(3),e.getLong(4)))
          sumInfoByVersion.collect().foreach(i=>{
            util.insert(sqlInsert,date,i._1,"All",i._2,i._3,"All","All",new JLong(i._4),new JLong
            (i._5))
          })

          cal.add(Calendar.DAY_OF_MONTH,-1)
          calDir.add(Calendar.DAY_OF_MONTH,-1)
          logData.unpersist()
        })
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
