package com.moretv.bi.login

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

/**
  * Created by zhangyu on 2016/7/18.
  * 统计分渠道的yunos日新增、活跃用户数，采用mac去重方式。
  * tablename: medusa.medusa_yunos_based_promotionchannel
  *          (id,day,promotion_channel,adduser_num,active_num)
  * Params : startDate, numOfDays(default = 1);
  *
  */
object YunOSBasedPromotionChannel extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(YunOSBasedPromotionChannel,args)
  }
  override def  execute(args:Array[String]): Unit ={
    ParamsParseUtil.parse(args) match{
      case Some(p) => {

        val util = DataIO.getMySqlOps("moretv_medusa_mysql")


        val cal1 = Calendar.getInstance()
        cal1.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal1.add(Calendar.DAY_OF_MONTH,-1)

        val cal2 = Calendar.getInstance()
        cal2.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val addlogdate = DateFormatUtils.readFormat.format(cal1.getTime)
          val addlogpath = s"/log/dbsnapshot/parquet/$addlogdate/moretv_mtv_account"
          val addtimeday = DateFormatUtils.toDateCN(addlogdate)
          val startTime = s"$addtimeday"+" "+"00:00:00"
          val endTime = s"$addtimeday"+" "+"23:59:59"


          val activelogdate = DateFormatUtils.readFormat.format(cal2.getTime)
          val activelogpath = s"/log/moretvloginlog/parquet/$activelogdate/loginlog"
          //val activetimeday = DateFormatUtils.toDateCN(activelogdate,-1)

          sqlContext.read.load(addlogpath).
            select("current_version","openTime","mac","promotion_channel").
            registerTempTable("addlog_data")

          val addmap = sqlContext.sql(s"select promotion_channel,count(distinct mac) from addlog_data " +
            s"where openTime between '$startTime' and '$endTime' " +
            s"and (current_version like '%YunOS%' or current_version like 'Alibaba')" +
            s"group by promotion_channel").
            collectAsList().
            map(e =>
              ({if(e.getString(0)== null) "null" else if(e.getString(0).isEmpty()) "kong" else e.getString(0)},e.getLong(1))).toMap

          sqlContext.read.load(activelogpath).
            select("version","mac","promotionChannel").registerTempTable("activelog_data")

          val activemap = sqlContext.sql(s"select promotionChannel,count(distinct mac) from activelog_data " +
            s"where version like '%YunOS%' or version like '%Alibaba%' group by promotionChannel").
            collectAsList().
            map(e =>
              ({if(e.getString(0)== null) "null" else if(e.getString(0).isEmpty()) "kong" else e.getString(0)},e.getLong(1))).toMap



//          val intersectionRdd = (addrdd join(activerdd)).map(e=>(e._1,(e._2._1,e._2._2)))
//
//          val addRemainingRdd = addrdd.subtract(intersectionRdd.map(e=>(e._1,e._2._1))).map(e=>(e._1,(e._2,0.toLong)))
//
//          val activeRemainingRdd = activerdd.subtract(intersectionRdd.map(e=>(e._1,e._2._2))).map(e=>(e._1,(0.toLong,e._2)))
//
//          val mergeRdd = intersectionRdd union addRemainingRdd union activeRemainingRdd

          if(p.deleteOld){
            val deleteSql = "delete from medusa_yunos_based_promotionchannel where day = ?"
            util.delete(deleteSql,addtimeday)
          }

          val insertSql = "insert into medusa_yunos_based_promotionchannel(day,promotion_channel,adduser_num,active_num) " +
            "values(?,?,?,?)"

          val keys = addmap.keySet.union(activemap.keySet)
          keys.foreach(key => {
            val adduser_num = addmap.getOrElse(key,0)
            val active_num = activemap.getOrElse(key,0)
            util.insert(insertSql,addtimeday,key,adduser_num,active_num)
          })

          cal1.add(Calendar.DAY_OF_MONTH,-1)
          cal2.add(Calendar.DAY_OF_MONTH,-1)
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }

  }
}

