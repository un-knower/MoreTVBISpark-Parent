package com.moretv.bi.login

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
/**
 * Created by HuZhehua on 2016/4/13.
 */

/**统计阿里狗和电视狗用户每日的活跃用户数和访问次数，以及新增用户数
  * Params : startDate, numOfDays(default = 1);
  * @return : insert into bi.alidog_tvdog_active_newuser, (day,version,active_user_num,access_num,new_user_num);
  */
object AliDogTVDogActiveAndNewUserStatistics extends BaseClass{
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val db = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=> {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val path = "/log/moretvloginlog/parquet/"+date+"/loginlog"
          val sourceRdd = sqlContext.read.load(path).select("version","mac").cache()
          val aliDogActiveUserNum = sourceRdd.filter("version like 'MoreTV_TVApp2.0_Android_YunOS2_%'").select("mac").distinct().count()
          val aliDogActiveAccessNum = sourceRdd.filter("version like 'MoreTV_TVApp2.0_Android_YunOS2_%'").select("mac").count()
          val tvDogActiveUserNum = sourceRdd.filter("version like 'MoreTV_TVApp2.0_Android_YunOS_%'").select("mac").distinct().count()
          val tvDogActiveAccessNum = sourceRdd.filter("version like 'MoreTV_TVApp2.0_Android_YunOS_%'").select("mac").count()
          val sqlQueryAliDog = "SELECT COUNT(DISTINCT mac) FROM mtv_account WHERE openTime >= '"+day+" 00:00:00' AND openTime <= '"+day+" 23:59:59' " +
            "AND current_version LIKE 'MoreTV_TVApp2.0_Android_YunOS2_%'"
          val sqlQueryTVDog = "SELECT COUNT(DISTINCT mac) FROM mtv_account WHERE openTime >= '"+day+" 00:00:00' AND openTime <= '"+day+" 23:59:59' " +
            "AND current_version LIKE 'MoreTV_TVApp2.0_Android_YunOS_%'"
          val aliDogNewUser = db.selectOne(sqlQueryAliDog)(0).toString.toLong
          val tvDogNewUser = db.selectOne(sqlQueryTVDog)(0).toString.toLong
          if(p.deleteOld){
            val sqlDelete = "DELETE FROM alidog_tvdog_active_newuser WHERE day = ?"
            util.delete(sqlDelete,day)
          }
          val sqlInsert = "INSERT INTO alidog_tvdog_active_newuser(day,version,active_user_num,access_num,new_user_num) VALUES(?,?,?,?,?)"
          util.insert(sqlInsert, day, "阿里狗", new JLong(aliDogActiveUserNum), new JLong(aliDogActiveAccessNum), new JLong(aliDogNewUser))
          util.insert(sqlInsert, day, "电视狗", new JLong(tvDogActiveUserNum), new JLong(tvDogActiveAccessNum), new JLong(tvDogNewUser))
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
        db.destory()
        util.destory()
      }
      case None => throw new RuntimeException("Need at least one param")
    }
  }
}