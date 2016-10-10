package com.moretv.bi.kidslogin

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/2/16.
  */
object KidsPromotionChannelDetail extends BaseClass{

  val regex = "^\\w+$".r

  def main(args: Array[String]) {
    ModuleClass.executor(KidsPromotionChannelDetail,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val inputPath = s"/log/mtvkidsloginlog/parquet/$inputDate/loginlog"
        val day = DateFormatUtils.toDateCN(inputDate, -1)

        val logRdd = sqlContext.read.load(inputPath).select("promotionChannel","mac").
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
        val loginNums = logRdd.countByKey()

        val userNums = logRdd.distinct().countByKey()

        val dbTvService = new DBOperationUtils("tvservice")
        val pcSql = "SELECT ifnull(promotion_channel,'null') as pchannel, COUNT(DISTINCT mac) as new_num " +
          s"FROM mtv_kid_account WHERE openTime BETWEEN '$day 00:00:00' AND '$day 23:59:59' GROUP BY promotion_channel"

        val pcMap = dbTvService.selectArrayList(pcSql).map(arr => (arr(0).toString,arr(1).toString.toInt)).toMap

        val db = new DBOperationUtils("bi")
        if(p.deleteOld){
          val sqlDelete = "delete from mtv_kid_promotion_detail where day = ?"
          db.delete(sqlDelete,day)
        }

        val sqlInsert = "insert into mtv_kid_promotion_detail(year,month,day, promotion_channel,new_num,user_num, login_num) " +
        " values(?,?,? ,?,?,? ,?)"
        val year = day.substring(0,4).toInt
        val month = day.substring(5,7).toInt
        userNums.foreach(x => {
          val promotionChannel = x._1
          val usernum = x._2
          val loginnum = loginNums(promotionChannel)
          val newnum = pcMap.getOrElse(promotionChannel,0)
          db.insert(sqlInsert,new Integer(year),new Integer(month),day,promotionChannel,new Integer(newnum),new Integer(usernum.toInt),new Integer(loginnum.toInt))
        })

        println("userNums size: "+userNums.size)
        println("pcMap size: "+pcMap.size)
        println("pcMap intersection userNums: "+pcMap.keySet.intersect(userNums.keySet).size)

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
