package com.moretv.bi.temp

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.bi.utils.ElasticSearchUtil
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by liankai on 2016/5/16.
  *
  */
object TencentVideoPlayNum extends BaseClass{

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
          val day = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)


          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MERGER,LogTypes.PLAYVIEW,date).
            select("userId","contentType","event","videoSid")
            .registerTempTable("log_data")

          sqlContext.sql("select videoSid,contentType,count(userId) as play_num,count(distinct userId) as play_user" +
            " from log_data where videoSid is not null and event in ('startplay','playview') and contentType in ('movie','tv','zongyi'," +
            "'comic','jilu','kids') group by contentType,videoSid").registerTempTable("log_play_num")

          sqlContext.read.load(s"/log/temp/parquet/$startDate/mtv_tencent").registerTempTable("mtv_tencent")

          val result = sqlContext.sql("select a.contentType,sum(a.play_num) as play_num ,sum(a.play_user) as play_user from log_play_num a join mtv_tencent b on " +
            "a.videoSid = b.sid group by a.contentType").collect()
          if(p.deleteOld){
            util.delete("delete from temp_tencent_play_num where day = ?",day)
          }
          val insertSql="insert into temp_tencent_play_num(day,content_type,play_num,play_user) " +
            "values (?,?,?,?)"

          result.foreach(row => {
            println(row)
            util.insert(insertSql,day,row.get(0),row.get(1),row.get(2))
          })



        })
        util.destory()
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
