package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions.col

/**
  * Created by zhu.bingxin on 2017/4/26.
  * 统计活跃用户数和登录用户数
  * 区分新老版本（3.1.4及以上的为新版本）
  */
object activeUserAndLoginUserCt extends BaseClass {


  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  /**
    * this method do not complete.Sub class that extends BaseClass complete this method
    */
  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH, -1)
        (0 until p.numOfDays).foreach(i => {

          //define the day
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDate = DateFormatUtils.toDateCN(date)

          //define database
          val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

          //add 1 day after read currrent day
          cal.add(Calendar.DAY_OF_MONTH, 1)
          val date2 = DateFormatUtils.readFormat.format(cal.getTime)

          //load data
          val logAccount = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, date)
          val logLogin = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETVLOGINLOG, LogTypes.LOGINLOG, date2)

          //filter data
          logAccount.select("date", "happenTime", "host", "apkVersion", "productModel", "promotionChannel", "userId", "nextPageId",
            "contentType", "order", "condition", "appEnterWay")
            //.withColumn("happenDate", udfToDateCN(col("happenTime").substr(1, 8)))
            //cut out the happenTime column,and then change it to standard date
            .registerTempTable("log_table")

          //data processings
          val resultDf = sqlContext.sql(
            """
              |select date,happenDate,host,apkVersion,productModel,promotionChannel,userId,nextPageId,contentType
              |,order as orderType,condition as conditionList,appEnterWay,count(1) as pv
              |from log_table
              |group by date,happenDate,host,apkVersion,productModel,promotionChannel,userId,nextPageId,contentType,order
              |,condition,appEnterWay
            """.stripMargin)

          val insertSql = "insert into eagle_filter_click_statistics(date,happenDate,host,apkVersion,productModel," +
            "promotionChannel,userId,nextPageId,contentType,orderType,conditionList,appEnterWay,pv) " +
            "values (?,?,?,?,?,?,?,?,?,?,?,?,?)"
          if (p.deleteOld) {
            val deleteSql = "delete from eagle_filter_click_statistics where date=?"
            util.delete(deleteSql, insertDate)
          }

          resultDf.collect.foreach(e => {
            util.insert(insertSql, e.getString(0), e.getString(1), e.getString(2), e.getString(3), e.getString(4),
              e.getString(5), e.getString(6), e.getString(7), e.getString(8), e.getString(9), e.getString(10),
              e.getString(11), e.getLong(12))
          })
          println(insertDate + " Insert data successed!")
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }
}
