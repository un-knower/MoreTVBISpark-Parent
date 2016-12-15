package com.moretv.bi.user

import java.sql.SQLException

import com.moretv.bi.util.SparkSetting
import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.JavaConversions._
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/7/10.
  * 统计各终端的新增用户的渠道分布
  */
object ProductBrandPromotionChannelNewDist extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(ProductBrandPromotionChannelNewDist,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = DateFormatUtils.enDateAdd(p.startDate,-1)
        val inputPath = s"/log/dbsnapshot/parquet/$inputDate/moretv_mtv_account"
        val day = DateFormatUtils.toDateCN(p.startDate, -1)
        sqlContext.udf.register("getBrand",ProductModelUtils.getProductBrand _)

        sqlContext.read.load(inputPath).registerTempTable("log_data")

        val result = sqlContext.sql("select getBrand(product_model),promotion_channel,count(distinct mac) from log_data " +
          s"where openTime between '$day 00:00:00' and '$day 23:59:59' group by getBrand(product_model),promotion_channel").
          collectAsList()

        val db = DataIO.getMySqlOps("moretv_medusa_mysql")
        if(p.deleteOld){
          val sqlDelete = "delete from product_brand_promotion_channel_new_dist where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = "insert into product_brand_promotion_channel_new_dist " +
          "(day,product_brand,promotion_channel,user_num,rate) values(?,?,?,?,?)"
        result.groupBy(row => row.getString(0)).foreach(x => {
          val totalNewNum = x._2.map(row => row.getLong(2)).sum
          x._2.foreach(t => {
            try {
              val newNum = t.getLong(2)
              val rate = newNum.toDouble/totalNewNum
              val promotionChannel = if(t.getString(1) == null) "null" else t.getString(1)
              db.insert(sqlInsert,day,t.get(0),promotionChannel,t.getLong(2),rate)
            } catch {
              case e:SQLException =>
                if(e.getMessage.contains("Data too long"))
                  println(e.getMessage)
                else throw new RuntimeException(e)
              case e:Exception => throw new RuntimeException(e)
            }
          })
        })

        db.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
