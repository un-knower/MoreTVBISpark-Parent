package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object MedusaEnterInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate


        val medusaDir = "/log/medusa/parquet"
        val enterLogType = "enter"


        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaEnterInput = s"$medusaDir/$date/$enterLogType/"

          val medusaEnterlog = sqlContext.read.parquet(medusaEnterInput)

          medusaEnterlog.select("userId","apkVersion","buildDate","productModel").registerTempTable("medusa_enter_log")
          sqlContext.read.load(s"/log/medusa/parquet/$date/*").registerTempTable("log_data")
          val medusaEnterUserRdd=sqlContext.sql("select buildDate,productModel,count(distinct userId) from log_data " +
            "where apkVersion='3.0.6' group by buildDate,productModel").map(e=>(e.getString(0),e.getString(1),e.getLong(2)))
            .map(e=>((e._1,e._2),e._3))

          val medusaEnterNumRdd = sqlContext.sql("select buildDate,productModel,count(userId) from medusa_enter_log " +
            "where userId not like '999999999999%' and apkVersion='3.0.6' group by buildDate,productModel")
            .map(e=>(e.getString(0),e.getString(1),e.getLong(2))).map(e=>((e._1,e._2),e._3))
          val medusaEnterInfoRdd = medusaEnterUserRdd.join(medusaEnterNumRdd).collect()



          val sqlInsert = "insert into medusa_gray_testing_denglu_num_user_each_product(day,apkVersion,buildDate," +
            "product_code, enter_num,enter_user) values (?,?,?,?,?,?)"

          medusaEnterInfoRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,"3.0.6",e._1._1,e._1._2,new JLong(e._2._2),new JLong(e._2._1))

          })


          val medusaEnterAllUserRdd=sqlContext.sql("select productModel,count(distinct userId) from log_data " +
            "where apkVersion='3.0.6' group by productModel").map(e=>(e.getString(0),e.getLong(1))).map(e=>(e._1,e
            ._2))

          val medusaEnterAllNumRdd = sqlContext.sql("select productModel,count(userId) from medusa_enter_log " +
            "where userId not like '999999999999%' and apkVersion='3.0.6' group by productModel")
            .map(e=>(e.getString(0),e.getLong(1))).map(e=>(e._1,e._2))
          val medusaEnterAllInfoRdd = medusaEnterAllUserRdd.join(medusaEnterAllNumRdd).collect()

          medusaEnterAllInfoRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,"3.0.6","All",e._1,new JLong(e._2._2),new JLong(e._2._1))
          })


        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
