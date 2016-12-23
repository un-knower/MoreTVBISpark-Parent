package com.moretv.bi.temp.user

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by zhangyu on 16/9/6.
 * 统计日活用户中阿里云版本和非阿里云版本
 * table_name : login_user_based_yunos
 * (id,day,version_type,user_num)
 * version分为全部 阿里云和非阿里云
 */
object LoginUserBasedYunOS extends BaseClass{

  def main(args: Array[String]): Unit ={
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) = {
    ParamsParseUtil.parse(args) match{
      case Some(p) => {
        val util = DataIO.getMySqlOps("moretv_medusa_mysql")

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(x => {
          val logDay = DateFormatUtils.readFormat.format(cal.getTime)
          val logPath = s"/log/moretvloginlog/parquet/$logDay/loginlog"

          val sqlDay = DateFormatUtils.toDateCN(logDay, -1)

          sqlContext.read.load(logPath).registerTempTable("log_data")

          val yunosUserNum = sqlContext.sql("select distinct mac from log_data " +
            "where version like '%YunOS%' or version like '%Alibaba%'").count()

          val allUserNum = sqlContext.sql("select distinct mac from log_data").count()

          val nonYunosUserNum = allUserNum - yunosUserNum

          if(p.deleteOld) {
            val deleteSql = "delete from login_user_based_yunos where day = ?"
            util.delete(deleteSql,logDay)
          }

          val insertSql = "insert into login_user_based_yunos(day,version_type,user_num) values(?,?,?)"
          util.insert(insertSql,sqlDay,"全部",allUserNum)
          util.insert(insertSql,sqlDay,"阿里云",yunosUserNum)
          util.insert(insertSql,sqlDay,"非阿里云",nonYunosUserNum)

          cal.add(Calendar.DAY_OF_MONTH,-1)
        })


    }
      case None => {
        throw new RuntimeException("At least need one param: startDate!")
      }
    }
  }

}
