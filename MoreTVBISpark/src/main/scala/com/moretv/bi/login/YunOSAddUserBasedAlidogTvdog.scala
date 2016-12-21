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
/**
  * Created by zhangyu on 2016/7/14.
  * 统计分版本（阿里狗、电视狗,阿里巴巴）的yunos日新增用户数，采用mac去重方式。
  * tablename: medusa.medusa_yunos_adduser_based_alidogtvdog (id,day,version_type.adduser_num)
  * Params : startDate, numOfDays(default = 1);
  *
  */
object YunOSAddUserBasedAlidogTvdog extends BaseClass{
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }
  override def  execute(args:Array[String]): Unit ={
    ParamsParseUtil.parse(args) match{
      case Some(p) => {

        val util = DataIO.getMySqlOps("moretv_medusa_mysql")

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH,-1)

        (0 until p.numOfDays).foreach(i => {
          val logdate = DateFormatUtils.readFormat.format(cal.getTime)
          val logpath = s"/log/dbsnapshot/parquet/$logdate/moretv_mtv_account"
          val timeday = DateFormatUtils.toDateCN(logdate)
          val startTime = s"$timeday"+" "+"00:00:00"
          val endTime = s"$timeday"+" "+"23:59:59"

          sqlContext.read.load(logpath).
            select("openTime","current_version","mac").registerTempTable("log_data")

          val allAddUserNum = sqlContext.sql(s"select distinct mac from log_data " +
            s"where openTime between '$startTime' and '$endTime' and " +
            s"(current_version like '%YunOS%' or current_version like '%Alibaba%')").count()

          val aliDogAddUserNum = sqlContext.sql(s"select mac,current_version from log_data " +
            s"where openTime between '$startTime' and '$endTime'").
            map(row => {
              val mac = row.getString(0)
              val version = row.getString(1)
              if(version != null && version.contains("_YunOS_")) mac else null
            }).
            filter(_ != null).
            distinct().count()

          val tvDogAddUserNum = sqlContext.sql(s"select mac,current_version from log_data " +
            s"where openTime between '$startTime' and '$endTime'").
            map(row => {
              val mac = row.getString(0)
              val version = row.getString(1)
              if(version != null && version.contains("_YunOS2")) mac else null
            }).
            filter(_ != null).
            distinct().count()

          val alibabaAddUserNum = allAddUserNum - aliDogAddUserNum - tvDogAddUserNum


          if(p.deleteOld){
            val deleteSql = "delete from medusa_yunos_adduser_based_alidogtvdog " +
              "where day = ?"
            util.delete(deleteSql, timeday)
          }

          val insertSql = "insert into medusa_yunos_adduser_based_alidogtvdog(day,version_type,adduser_num) " +
            "values(?,?,?)"
          util.insert(insertSql,timeday,"ALL",allAddUserNum)
          util.insert(insertSql,timeday,"阿里狗",aliDogAddUserNum)
          util.insert(insertSql,timeday,"电视狗",tvDogAddUserNum)
          util.insert(insertSql,timeday,"阿里巴巴",alibabaAddUserNum)


          cal.add(Calendar.DAY_OF_MONTH,-1)
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
