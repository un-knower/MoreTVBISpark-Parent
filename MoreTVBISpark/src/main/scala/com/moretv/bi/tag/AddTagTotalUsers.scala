package com.moretv.bi.tag

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by Will on 2015/4/18.
 */
object AddTagTotalUsers extends BaseClass{

  def main(args: Array[String]) {
    config.setAppName("AddTagTotalUsers")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val yesterday = Calendar.getInstance()
        val formatCN = new SimpleDateFormat("yyyy-MM-dd")
        yesterday.add(Calendar.DAY_OF_MONTH, -p.whichDay)
        val yesterdayCN = formatCN.format(yesterday.getTime)

        val programMap = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.OPERATION_ACW,p.startDate).
          filter("event='addtag'").select("userId").map(e=>e.getString(0)).distinct()
        val sqlMinMaxId = "select min(id),max(id) from addTagTotalUserId"
        val sqlData = "SELECT accountId FROM `addTagTotalUserId` WHERE ID >= ? AND ID <= ?"
        val userIdRDD = MySqlOps.getJdbcRDD(sc,DataBases.MORETV_BI_MYSQL,sqlMinMaxId,sqlData,10,r=>r.getString(1)).distinct()

        val resultRDD = programMap.subtract(userIdRDD)

        //save date
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from addTagTotalUsers where day = '$date'"
          util.delete(oldSql)
        }
        val sql = "INSERT INTO bi.addTagTotalUserId(day,userId) values(?,?)"
        val result = resultRDD.collect()
        result.foreach(x => {
          util.insert(sql,yesterdayCN,x)
        })
        //保存累计人数
        val sql2 = "INSERT INTO bi.addTagTotalUsers(day,user_num) select '"+ yesterdayCN +"', count(0) from bi.addTagTotalUserId where day <= '" + yesterdayCN +"'"
        util.insert(sql2)
        util.destory()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }
}
