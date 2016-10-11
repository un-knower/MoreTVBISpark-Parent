package com.moretv.bi.user

import java.sql.SQLException

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, ProductModelUtils}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}



/**
  * Created by Will on 2016/7/10.
  * 统计各终端的新增用户的渠道分布
  */
object PromotionChannelNonOTTNewDist extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(PromotionChannelNonOTTNewDist,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = DateFormatUtils.enDateAdd(p.startDate,-1)
        val inputPath = s"/log/dbsnapshot/parquet/$inputDate/moretv_mtv_account"
        val day = DateFormatUtils.toDateCN(p.startDate, -1)

        sqlContext.read.load(inputPath).registerTempTable("log_data")

        val result = sqlContext.sql("select product_model,promotion_channel,mac from log_data " +
          s"where openTime between '$day 00:00:00' and '$day 23:59:59' ").
          map(row => {
            val productModel = row.getString(0)
            if(!ProductModelUtils.isOtt(productModel)) (row.getString(1),row.getString(2)) else null
          }).filter(_!=null).distinct().countByKey()

        val db = DataIO.getMySqlOps("moretv_medusa_mysql")
        if(p.deleteOld){
          val sqlDelete = "delete from promotion_channel_non_ott_new_dist where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = "insert into promotion_channel_non_ott_new_dist " +
          "(day,promotion_channel,user_num) values(?,?,?)"
        result.foreach(row => {
          val promotionChannel = if(row._1 == null) "null" else row._1

          try {
            db.insert(sqlInsert,day,promotionChannel,row._2)
          } catch {
            case e:SQLException =>
              if(e.getMessage.contains("Data too long"))
                println(e.getMessage)
              else throw new RuntimeException(e)
            case e:Exception => throw new RuntimeException(e)
          }
        })

        db.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
