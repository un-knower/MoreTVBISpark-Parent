package com.moretv.bi.common

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

object PrevueVVUV extends BaseClass {

  def main(args: Array[String]) {
    config.setAppName("PrevueVVUV")
    ModuleClass.executor(PrevueVVUV, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cacheValue = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.OPERATION_ACW)
          .filter("event='watchprevue'").select("date", "videoSid", "userId").
          map(e => ((e.getString(1), e.getString(0)), e.getString(2))).persist()
        //mapPartitions(p => {
        //val util = new ProgramRedisUtils
        // p.map(log => matchLog(util,log))
        //}).filter(_!=null).map(x => ((x._1, x._2, x._3), x._4)).persist()
        val util = new ProgramRedisUtils
        val userNumValue = cacheValue.distinct().countByKey()
        val accessNumValue = cacheValue.countByKey()

        val sql = "insert into prevueVVUV(sid,title,day,user_num,user_access) values(?,?,?,?,?)"
        val dbUtil = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from prevueVVUV where day = '$date'"
          dbUtil.delete(oldSql)
        }
        userNumValue.foreach(
          x => {
            val title = util.getTitleBySid(x._1._1)
            dbUtil.insert(sql, x._1._1, title, x._1._2, new Integer(x._2.toInt), new Integer(accessNumValue(x._1).toInt))
          }
        )
        dbUtil.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
}
