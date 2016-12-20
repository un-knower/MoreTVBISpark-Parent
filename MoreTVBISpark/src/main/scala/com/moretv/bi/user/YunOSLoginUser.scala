package com.moretv.bi.user

import java.lang.{Long => JLong}
import java.util.regex.Pattern

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext

/**
 * Created by Will on 2015/2/5.
 */
object YunOSLoginUser extends BaseClass{

  val pattern = Pattern.compile("/login/Service/(login|enlogin)\\?.?" +
    "mac=([a-zA-Z0-9]{12}).+?version=MoreTV[\\w\\.]+(Alibaba|YunOS)")

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "2g").
      set("spark.cores.max", "30").
      set("spark.storage.memoryFraction", "0.6")
    ModuleClass.executor(YunOSLoginUser,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {



        val inputDate =  p.startDate
        val logRDD = sc.textFile(s"/log/loginlog/loginlog.access.log_$inputDate-portalu*").
          map(matchLog).filter(_ != null).cache()

        val accessNum = logRDD.count()
        val userNum = logRDD.distinct().count()

        logRDD.unpersist()
        sc.stop()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if(p.deleteOld) {
          val oldSql = s"delete from bi.yunos_login_user where day = '$day'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO bi.yunos_login_user(day,user_num,access_num) VALUES(?,?,?)"
        util.insert(sql,day,new JLong(userNum),new JLong(accessNum))
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

  def matchLog(log:String) ={
    val matcher = pattern.matcher(log)
    if(matcher.find()) matcher.group(2) else null
  }

}
