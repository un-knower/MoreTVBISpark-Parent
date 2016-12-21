package com.moretv.bi.user

import java.sql.SQLException
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/7/10.
  * 统计各终端的新增用户的渠道分布
  */
object PromotionChannelTotalDist extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //val inputDate = DateFormatUtils.enDateAdd(p.startDate,-1)
        //val day = DateFormatUtils.toDateCN(p.startDate, -1)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH,-1)



        (0 until p.numOfDays).foreach(e=> {

          val logDay = DateFormatUtils.readFormat.format(cal.getTime)
         // println(logDay)


          val day = DateFormatUtils.toDateCN(logDay)
          //println(day)


          DataIO.getDataFrameOps.getDF(sc,p.paramMap,DBSNAPSHOT,LogTypes.MORETV_MTV_ACCOUNT,logDay).
            registerTempTable("log_data")

          val result = sqlContext.sql("select promotion_channel,count(distinct mac) from log_data " +
            s"where openTime <= '$day 23:59:59' group by promotion_channel").
            collectAsList()

          val db = DataIO.getMySqlOps("moretv_medusa_mysql")
          if(p.deleteOld){
            val sqlDelete = "delete from promotion_channel_total_dist where day = ?"
            db.delete(sqlDelete,day)
          }

          val sqlInsert = "insert into promotion_channel_total_dist " +
            "(day,promotion_channel,user_num) values(?,?,?)"
          result.foreach(row => {
            val promotionChannel = if(row.getString(0) == null) "null" else if(row.getString(0) == "") "kong" else row.getString(0)

            try {
              db.insert(sqlInsert,day,promotionChannel,row.get(1))
            } catch {
              case e:SQLException =>
                if(e.getMessage.contains("Data too long"))
                  println(e.getMessage)
                else throw new RuntimeException(e)
              case e:Exception => throw new RuntimeException(e)
            }
          })

          //db.destory()
          cal.add(Calendar.DAY_OF_MONTH,-1)
        })


      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
