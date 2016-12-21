package com.moretv.bi.ProgramViewAndPlayStats

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object SubchannelPeriod extends BaseClass with DateUtil{

  def main(args: Array[String]): Unit = {
    config.setAppName("SubchannelPeriod")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val df1 = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.PLAYVIEW).persist(StorageLevel.MEMORY_AND_DISK)
        val playRDD = df1.select("datetime", "path","userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
            map(e => (getKeys(e._1, e._2),e._3)).filter(e =>(e._1._6 !=null && e._1._7 !=null && e._1._8 != null)).persist(StorageLevel.MEMORY_AND_DISK)
        val df2 = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.DETAIL).persist(StorageLevel.MEMORY_AND_DISK)
        val detailRDD = df2.select("datetime","path","userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
            map(e => (getKeys(e._1, e._2, "detail"),e._3)).filter(e =>(e._1._6 !=null && e._1._7 !=null && e._1._8 != null)).persist(StorageLevel.MEMORY_AND_DISK)

        val result = playRDD.union(detailRDD).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = result.distinct().countByKey()
        val accessNum = result.countByKey()

        //save date
        val util = DataIO.getMySqlOps(DataBases.MORETV_EAGLETV_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from subchannel_period where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO subchannel_period(year,month,day,weekstart_end,type,channel,subchannel_code,subchannel_name,period,user_num,access_num) VALUES(?,?,?,?,?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          println(x.toString())
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,x._1._7,x._1._8,x._1._9,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })

        playRDD.unpersist()
        result.unpersist()
        detailRDD.unpersist()
        df1.unpersist()
        df2.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def getKeys(date:String, path:String, logType:String = "playview")={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val hour = date.substring(11,13)
    val week = getWeekStartToEnd(date)

    var contentType:String = null
    var subChannel:String = null
    var subChannelName:String = null

    val reg = "(home|thirdparty_\\d{1})-(movie|tv|zongyi|comic|kids|jilu|kids_home)-(\\w+)".r
    val pattern = reg findFirstMatchIn path
    pattern match {
      case Some(x) =>
        contentType = x.group(2)
        subChannel = x.group(3)
        subChannelName = CodeToNameUtils.getSubChannelNameByCode(subChannel)
      case None => null
    }

    (year,month,date.substring(0,11),week,logType,contentType,subChannel,subChannelName,hour)
  }
}
