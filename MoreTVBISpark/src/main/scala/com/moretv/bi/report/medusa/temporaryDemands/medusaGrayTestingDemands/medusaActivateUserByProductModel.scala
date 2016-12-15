package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.util.Calendar

import java.lang.{Long=>JLong}
import com.moretv.bi.util.{SparkSetting, DateFormatUtils, DBOperationUtils, ParamsParseUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/23.
 */
object medusaActivateUserByProductModel extends SparkSetting{

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

          val medusaEnterUserRdd = sqlContext.sql("select apkVersion,buildDate,productModel,count(userId),count(distinct " +
            "userId) from medusa_enter_log where userId not like '999999999999%' and buildDate is not null group " +
            "by apkVersion,buildDate,productModel").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getLong(3),e
            .getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()

          val medusaEnterUserAllProductRdd = sqlContext.sql("select apkVersion,buildDate,'All' as productModel,count" +
            "(userId),count(distinct userId) from medusa_enter_log where userId not like '999999999999%' and buildDate is " +
            "not null group by apkVersion, buildDate").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getLong(3),
            e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()



          val sqlInsert = "insert into medusa_gray_testing_enter_info_product_build_each_day(date,apk_version,buildDate," +
            "product_model,enter_num,enter_user) values (?,?,?,?,?,?)"

          medusaEnterUserRdd.foreach(r=>{
            util.insert(sqlInsert,insertDate,r._1,r._2,r._3,new JLong(r._4),new JLong(r._5))
          })

          medusaEnterUserAllProductRdd.foreach(r=>{
            util.insert(sqlInsert,insertDate,r._1,r._2,r._3,new JLong(r._4),new JLong(r._5))
          })

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
