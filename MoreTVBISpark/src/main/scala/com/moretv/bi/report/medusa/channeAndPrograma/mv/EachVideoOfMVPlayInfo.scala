package com.moretv.bi.report.medusa.channeAndPrograma.mv

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.bi.utils.ElasticSearchUtil
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by xiajun on 2016/5/16.
 *
 */
object EachVideoOfMVPlayInfo extends BaseClass{
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)


          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.PLAY,date).select("userId","contentType","event","videoSid")
            .registerTempTable("log_data")

          val rdd = sqlContext.sql("select contentType,videoSid,count(userId),count(distinct userId)" +
            " from log_data where event in ('startplay','playview') and contentType in ('mv') group by contentType," +
            "videoSid").map(e=>(e.getString(0),e.getString(1),e.getLong(2),e
            .getLong(3))).filter(_._2!=null).filter(_._2.length<=50).collect()



          val insertSql="insert into medusa_channel_each_video_play_info(day,channel,video_sid,title,play_num,play_user) " +
            "values (?,?,?,?,?,?)"

          if(p.deleteOld){
            val deleteSql="delete from medusa_channel_each_video_play_info where day=?"
            util.delete(deleteSql,insertDate)
          }

          rdd.foreach(e=>{
            util.insert(insertSql,insertDate,e._1,e._2,ProgramRedisUtil.getTitleBySid(e._2),new JLong(e._3),
            new JLong(e._4))
          })

        })
        ElasticSearchUtil.close
        util.destory()
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
