package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.bi.utils.ElasticSearchUtil
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by xiajun on 2016/5/16.
 *
 */
object EachVideoOfChannelPlayInfo extends BaseClass{

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


          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MERGER,LogTypes.PLAYVIEW,date).
            select("userId","contentType","event","videoSid")
            .registerTempTable("log_data")

          val rdd = sqlContext.sql("select contentType,videoSid,count(userId),count(distinct userId)" +
            " from log_data where event in ('startplay','playview') and contentType in ('movie','tv','hot','zongyi'," +
            "'comic','xiqu','jilu','kids') group by contentType,videoSid").map(e=>(e.getString(0),e.getString(1),e.getLong(2),e
            .getLong(3))).filter(_._2!=null).filter(_._2.length<=50)

          val insertSql="insert into medusa_channel_each_video_play_info(day,channel,video_sid,title,play_num,play_user) " +
            "values (?,?,?,?,?,?)"

          if(p.deleteOld){
            val deleteSql="delete from medusa_channel_each_video_play_info where day=?"
            util.delete(deleteSql,insertDate)
          }


          rdd.collect().foreach(e=>{
            val title=ProgramRedisUtil.getTitleBySid(e._2)
            try{
              util.insert(insertSql,insertDate,e._1,e._2,title,new JLong(e._3),
                new JLong(e._4))
            }catch {
              case e:java.sql.SQLException => {
                println(s"insert errror: $title")
              }
              case e:Exception =>
            }
          })
//
//          rdd.foreachPartition(partition=>{
//            val util1 = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
//            partition.foreach(e=>{
//              val title=ProgramRedisUtil.getTitleBySid(e._2)
//              try{
//                util1.insert(insertSql,insertDate,e._1,e._2,title,new JLong(e._3),
//                  new JLong(e._4))
//              }catch {
//                case e:java.sql.SQLException => {
//                  println(s"insert errror: $title")
//                }
//                case e:Exception => throw e
//              }
//            })
//          })

        })
        ElasticSearchUtil.close
        util.destory()
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
