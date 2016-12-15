package com.moretv.bi.login

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/2/16.
  */
object MtvPromotionChannelDetail extends BaseClass{

  val regex = "^\\w+$".r

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(MtvPromotionChannelDetail,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val inputDate = p.startDate
        val inputPathMoretv = s"/log/moretvloginlog/parquet/$inputDate/loginlog"
        val day = DateFormatUtils.toDateCN(inputDate, -1)

        val logRddMoretv = sqlContext.read.load(inputPathMoretv).select("promotionChannel","mac").
          map(row => {
            val promotionChannel = row.getString(0)
            if(promotionChannel != null){
              if(promotionChannel == ""){
                ("kong",row.getString(1))
              }else{
                regex findFirstIn promotionChannel match {
                  case Some(s) => (s,row.getString(1))
                  case None => ("corrupt",row.getString(1))
                }
              }
            }else ("null",row.getString(1))
          }).cache()

        val inputPathKids = s"/log/mtvkidsloginlog/parquet/$inputDate/loginlog"
        val logRddKids = sqlContext.read.load(inputPathKids).select("promotionChannel","mac").
          map(row => {
            val promotionChannel = row.getString(0)
            if(promotionChannel != null){
              if(promotionChannel == ""){
                ("kong",row.getString(1))
              }else{
                regex findFirstIn promotionChannel match {
                  case Some(s) => (s,row.getString(1))
                  case None => ("corrupt",row.getString(1))
                }
              }
            }else ("null",row.getString(1))
          })

        val logRdd = logRddMoretv union logRddKids
        val loginNums = logRdd.countByKey()

        val userNums = logRdd.distinct().countByKey()

        val dbTvService = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
        val pcSqlMoretv = "SELECT ifnull(promotion_channel,'null') as pchannel, COUNT(DISTINCT mac) as new_num " +
          s"FROM mtv_account WHERE openTime BETWEEN '$day 00:00:00' AND '$day 23:59:59' GROUP BY promotion_channel"

        val pcMapMoretv = dbTvService.selectArrayList(pcSqlMoretv).map(arr => {
          val promotionChannel = arr(0).toString
          val newNum = arr(1).toString.toInt
          if(promotionChannel == "") ("kong",newNum) else (promotionChannel,newNum)
        }).toMap

        val pcSqlKids = "SELECT ifnull(promotion_channel,'null') as pchannel, COUNT(DISTINCT mac) as new_num " +
          s"FROM mtv_kid_account WHERE openTime BETWEEN '$day 00:00:00' AND '$day 23:59:59' GROUP BY promotion_channel"

        val pcMapKids = dbTvService.selectArrayList(pcSqlKids).map(arr => {
          val promotionChannel = arr(0).toString
          val newNum = arr(1).toString.toInt
          if(promotionChannel == "") ("kong",newNum) else (promotionChannel,newNum)
        }).toMap

        val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        if(p.deleteOld){
          val sqlDelete = "delete from mtv_promotion_detail where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = "insert into mtv_promotion_detail(year,month,day, promotion_channel,new_num,user_num, login_num) " +
        " values(?,?,? ,?,?,? ,?)"
        val year = day.substring(0,4).toInt
        val month = day.substring(5,7).toInt
        val keys = userNums.keySet.union(pcMapMoretv.keySet).union(pcMapKids.keySet)
        keys.foreach(key => {
          val promotionChannel = key
          val usernum = userNums.getOrElse(promotionChannel,0L)
          val loginnum = loginNums.getOrElse(promotionChannel,0L)
          val newnum = pcMapMoretv.getOrElse(promotionChannel,0) + pcMapKids.getOrElse(promotionChannel,0)
          db.insert(sqlInsert,new Integer(year),new Integer(month),day,promotionChannel,new Integer(newnum),
            new Integer(usernum.toInt),new Integer(loginnum.toInt))
        })

        dbTvService.destory()
        db.destory()
        logRdd.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
