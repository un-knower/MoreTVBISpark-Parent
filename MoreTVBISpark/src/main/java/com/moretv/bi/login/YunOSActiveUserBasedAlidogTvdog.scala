package com.moretv.bi.login
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by zhangyu on 2016/7/15.
  * 统计分版本（阿里狗、电视狗，阿里巴巴）的yunos日活跃用户数，采用mac去重方式。
  * tablename: medusa.medusa_yunos_activeuser_based_alidogtvdog (id,day,version_type.active_num)
  * Params : startDate, numOfDays(default = 1);
  *
  */
object YunOSActiveUserBasedAlidogTvdog extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(YunOSActiveUserBasedAlidogTvdog,args)
  }
  override def  execute(args:Array[String]): Unit ={
    ParamsParseUtil.parse(args) match{
      case Some(p) => {

        val util = DataIO.getMySqlOps("moretv_medusa_mysql")

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))


        (0 until p.numOfDays).foreach(i => {
          val logdate = DateFormatUtils.readFormat.format(cal.getTime)
          val logpath = s"/log/moretvloginlog/parquet/$logdate/loginlog"
          val timeday = DateFormatUtils.toDateCN(logdate,-1)

          sqlContext.read.load(logpath).
            select("version","mac").registerTempTable("log_data")

          val allActiveNum = sqlContext.sql("select distinct mac from log_data " +
            "where version like '%YunOS%' or version like '%Alibaba%'").count()

          val aliDogActiveNum = sqlContext.sql("select mac,version from log_data").
            map(row => {
              val mac = row.getString(0)
              val version = row.getString(1)
              if(version != null && version.contains("_YunOS_")) mac else null
            }).
            filter(_ != null).
            distinct().count()

          val tvDogActiveNum = sqlContext.sql("select mac,version from log_data").
            map(row => {
              val mac = row.getString(0)
              val version = row.getString(1)
              if(version != null && version.contains("_YunOS2")) mac else null
            }).
            filter(_ != null).
            distinct().count()

          val alibabaActiveNum = allActiveNum - aliDogActiveNum - tvDogActiveNum



          if(p.deleteOld){
            val deleteSql = "delete from medusa_yunos_activeuser_based_alidogtvdog " +
              "where day = ?"
            util.delete(deleteSql, timeday)
          }

          val insertSql = "insert into medusa_yunos_activeuser_based_alidogtvdog(day,version_type,active_num) " +
            "values(?,?,?)"
          util.insert(insertSql,timeday,"ALL",allActiveNum)
          util.insert(insertSql,timeday,"阿里狗",aliDogActiveNum)
          util.insert(insertSql,timeday,"电视狗",tvDogActiveNum)
          util.insert(insertSql,timeday,"阿里巴巴",alibabaActiveNum)

          cal.add(Calendar.DAY_OF_MONTH,-1)
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
