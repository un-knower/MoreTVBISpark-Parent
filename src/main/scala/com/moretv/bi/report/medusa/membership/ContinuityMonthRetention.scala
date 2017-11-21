package com.moretv.bi.report.medusa.membership

import java.sql.{DriverManager, Statement}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global._
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
  * Created by xia.jun on 2017/11/17.
  * 统计每个连续包月用户的月留存情况
  * 主要包含两种情况：1、第一次购买连续包月的用户，在后续的月留存情况；2、用户取消连续包月后，又重新购买了连续包月的用户在后续的月留存情况
  */
object ContinuityMonthRetention extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val needToCalc = Array(31, 31, 31)
        val numOfDay = Array("oneMonth","twoMonth","threeMonth")

        /** 提取会员订单维度表*/
        val goodsDF = DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_MEMBER_GOODS).
          select("good_sk", "good_name").withColumnRenamed("good_sk", "dim_good_sk")

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH, -1)


        /** 提取累计订单信息*/
        val totalOrderDF = DataIO.getDataFrameOps.getDwFactAllDF(sqlContext, p.paramMap, DW_FACTS_ALL, FactTypes.FACT_MEDUSA_MEMBER_ORDER)
        val totalContMonOrderDF = totalOrderDF.join(goodsDF, totalOrderDF("good_sk") === goodsDF("dim_good_sk")).
          filter(s"good_name = '${FilterType.CONSECUTIVE_MONTH_ORDER}'").drop("good_name").drop("dim_good_sk").
          persist(StorageLevel.MEMORY_AND_DISK)

        (0 until p.numOfDays).foreach(i=>{

          val yesterday = DateFormatUtils.cnFormat.format(cal.getTime)
          val c = Calendar.getInstance()
          c.setTime(DateFormatUtils.cnFormat.parse(yesterday))

          cal.add(Calendar.DAY_OF_MONTH, 1)


          /** 提取今天生效的连续包月订单*/
          val todayActiveOrderDF = totalContMonOrderDF.filter(s"substring(vip_start_time,0,10) = '${yesterday}'").select("account_sk")

          // 创建插入数据库连接
          Class.forName("com.mysql.jdbc.Driver")
          val insertDB = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          val url = insertDB.prop.getProperty("url")
          val user = insertDB.prop.getProperty("user")
          val password = insertDB.prop.getProperty("password")
          val connection = DriverManager.getConnection(url,user,password)
          val stmt = connection.createStatement()

          (0 until needToCalc.length).foreach(j => {
            c.add(Calendar.DAY_OF_MONTH, - needToCalc(j))
            val date = DateFormatUtils.cnFormat.format(c.getTime)

            // 获取每天新增的连续包月的订单
            val newContMonOrderDF = totalContMonOrderDF.filter(s"is_first_cont_mon_order = 1 and substring(vip_start_time,0,10) = '${date}'").
              persist(StorageLevel.MEMORY_AND_DISK)

            val newUserNum = newContMonOrderDF.select("account_sk").distinct().count()
            val retention = newContMonOrderDF.join(todayActiveOrderDF, Seq("account_sk")).
              select("account_sk").distinct().count()
            val retentionRate = if(newUserNum == 0) 0.0 else retention.toDouble / newUserNum.toDouble

            if(p.deleteOld){
              deleteSQL(date,stmt)
            }
            if(j==0){
              insertSQL(date,newUserNum.toInt,retentionRate,stmt)
            }else{
              updateSQL(numOfDay(j),retentionRate,date,stmt)
            }

            newContMonOrderDF.unpersist()

          })
        })
        totalContMonOrderDF.unpersist()

      }

      case None => {println("参数有误！")}
    }

  }

  def insertSQL(date: String, count: Int, retention: Double,stmt: Statement) ={
    val sql = s"INSERT INTO medusa.`membership_cont_mon_retention_day` (day, new_user_num, oneMonth) VALUES('$date', $count, $retention)"
    stmt.executeUpdate(sql)
  }

  def updateSQL(num:String, retention:Double, date:String, stmt: Statement)={
    val sql = s"UPDATE medusa.`membership_cont_mon_retention_day` SET $num = $retention WHERE DAY = '$date'"
    stmt.executeUpdate(sql)
  }

  def deleteSQL(date:String,stmt:Statement) = {
    val sql = s"DELETE FROM medusa.`membership_cont_mon_retention_day` WHERE DAY = ${date}"
    stmt.execute(sql)
  }

}
