package com.moretv.bi.login

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

/**
  * Created by zhangyu and xiajun on 2016/7/20.
  * 统计分版本的日新增、活跃及累计用户数，采用mac去重方式。
  * tablename: medusa.medusa_user_statistics_based_apkversion
  *          (id,day,apk_version,adduser_num,accumulate_num,active_num)
  * Params : startDate, numOfDays(default = 1),deleteOld;
  *
  */
object UserStatisticsBasedApkVersion extends BaseClass{
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }
  override def  execute(args:Array[String]): Unit ={
    ParamsParseUtil.parse(args) match{
      case Some(p) => {

        val util = DataIO.getMySqlOps("moretv_medusa_mysql")

        //cal1代表用户数据库快照对应日期，cal2代表活跃用户库日志对应日期
        val cal1 = Calendar.getInstance()
        cal1.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal1.add(Calendar.DAY_OF_MONTH,-1)

        val cal2 = Calendar.getInstance()
        cal2.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val addLogdate = DateFormatUtils.readFormat.format(cal1.getTime)
          val addLogpath = s"/log/dbsnapshot/parquet/$addLogdate/moretv_mtv_account"
          val addTimeday = DateFormatUtils.toDateCN(addLogdate)
          val startTime = s"$addTimeday"+" "+"00:00:00"
          val endTime = s"$addTimeday"+" "+"23:59:59"

          sqlContext.read.load(addLogpath).
            select("current_version","openTime","mac").
            registerTempTable("addlog_data")

          val addMap = sqlContext.sql(s"select current_version, count(distinct mac) from addlog_data " +
            s"where openTime between '$startTime' and '$endTime' " +
            s"group by current_version").collectAsList().map(e =>
              ({if(e.getString(0)== null) "null" else if(e.getString(0).isEmpty()) "kong" else e.getString(0)},e.getLong(1))).toMap

          val accumulateMap = sqlContext.sql(s"select current_version, count(distinct mac) from addlog_data " +
            s"where openTime <= '$endTime' " +
            s"group by current_version").collectAsList().map(e =>
            ({if(e.getString(0)== null) "null" else if(e.getString(0).isEmpty()) "kong" else e.getString(0)},e.getLong(1))).toMap

          val activeLogdate = DateFormatUtils.readFormat.format(cal2.getTime)
          val activeLogpath = s"/log/moretvloginlog/parquet/$activeLogdate/loginlog"

          sqlContext.read.load(activeLogpath).
            select("mac","version").
            registerTempTable("activelog_data")

          val activeMap = sqlContext.sql(s"select version, count(distinct mac) from activelog_data group by version").collectAsList().map(e =>
            ({if(e.getString(0)== null) "null" else if(e.getString(0).isEmpty()) "kong" else e.getString(0)},e.getLong(1))).toMap


//          //合并三个RDD,考虑到RDD之间的数据传输影响性能，不采用这种方式。
//          val mergerRdd = addRdd union activeRdd union accumulateRdd
//
//          val mergerFormattedRdd = mergerRdd.groupBy(_._1).map(e=>(e._1,{
//            var t1 = 0L
//            var t2 = 0L
//            var t3 = 0L
//            e._2.foreach(i=>{
//              i._3 match {
//                case 1 => t1 = i._2
//                case 2 => t2 = i._2
//                case 3 => t3 = i._2
//                case _ =>
//              }
//            })
//            (t1,t2,t3)
//          })).map(e=>(e._1,e._2._1,e._2._2,e._2._3))

          if(p.deleteOld){
            val deleteSql = "delete from medusa_user_statistics_based_apkversion where day = ?"
            util.delete(deleteSql,addTimeday)
          }

          val insertSql = "insert into medusa_user_statistics_based_apkversion(day,apk_version,adduser_num,accumulate_num,active_num) values(?,?,?,?,?)"

          val keys = addMap.keySet.union(accumulateMap.keySet).union(activeMap.keySet)

          keys.foreach(key => {
            val adduser_num = addMap.getOrElse(key,0)
            val accumulate_num = accumulateMap.getOrElse(key,0)
            val active_num = activeMap.getOrElse(key,0)
            util.insert(insertSql,addTimeday,key,adduser_num,accumulate_num,active_num)
          }

          )
          cal1.add(Calendar.DAY_OF_MONTH,-1)
          cal2.add(Calendar.DAY_OF_MONTH,-1)
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }

  }
}
