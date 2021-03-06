package com.moretv.bi.user

import java.sql.SQLException
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/7/10.
  * 统计各终端的新增用户的渠道分布
  */
object ProductBrandPromotionChannelNewDist extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH,-1)
        (0 until p.numOfDays).foreach(i=>{

          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(inputDate)
          cal.add(Calendar.DAY_OF_MONTH,-1)


          DataIO.getDataFrameOps.getDF(sc,p.paramMap,DBSNAPSHOT,LogTypes.MORETV_MTV_ACCOUNT,inputDate).
            registerTempTable("log_data")

          DataIO.getDataFrameOps.getDimensionDF(
            sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PRODUCT_MODEL
          ).registerTempTable("dim_product")

          val result = sqlContext.sql(
            "select (case when b.brand_name is null then '其他品牌' else b.brand_name end), promotion_channel, count(distinct mac)" +
              " from log_data a left join dim_product b on a.product_model = b.product_model and b.dim_invalid_time is null" +
              s" where openTime between '$day 00:00:00' and '$day 23:59:59'" +
              " group by (case when b.brand_name is null then '其他品牌' else b.brand_name end), promotion_channel")
            .collectAsList()

          val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
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
        })
//        sqlContext.udf.register("getBrand",ProductModelUtils.getProductBrand _)

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
