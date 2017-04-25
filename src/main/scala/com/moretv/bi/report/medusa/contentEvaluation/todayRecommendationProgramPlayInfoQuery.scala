package com.moretv.bi.report.medusa.contentEvaluation

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DateFormatUtils, DBOperationUtils, ParamsParseUtil, ProgramRedisUtils}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
 * Created by Administrator on 2016/5/16.
 * 统计首页推荐的视频播放情况
 */
object todayRecommendationProgramPlayInfoQuery extends BaseClass{
  private val programRedisObj=new ProgramRedisUtils()

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        //val medusaDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

         /* val medusaDailyActiveInput = s"$medusaDir/$date/playview/"
          val medusaDailyActivelog = sqlContext.read.parquet(medusaDailyActiveInput).registerTempTable("medusa_daily_active_log")
*/
          DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MERGER,LogTypes.PLAYVIEW,date).registerTempTable("medusa_daily_active_log")

          val playInfoRdd=sqlContext.sql("select userId,videoSid,pathMain,path from medusa_daily_active_log where " +
            "launcherAreaFromPath in ('hotrecommend','recommendation') and event in ('startplay','playview')").map(e=>(e
            .getString(0),e.getString(1), e.getString(2),e.getString(3)))
          val filterRdd=playInfoRdd.filter(e=>{getPathLength(e._3,e._4)}).filter(_._1!=null).map(e=>(e._2,e._1))
            .persist(StorageLevel.MEMORY_AND_DISK)
          val playNumMap=filterRdd.countByKey().map(e=>(e._1,e._2))
          val playUserMap=filterRdd.distinct().countByKey().map(e=>(e._1,e._2))
          val playMergerRdd=playNumMap.map(e=>(e._1,e._2,playUserMap.getOrElse(e._1,0L)))

          val sqlInsert = "insert into medusa_content_evaluation_today_recommend_each_video_play_info(day,videoSid,title," +
            "play_num,play_user) values (?,?,?,?,?)"
          if(p.deleteOld){
            val deleteOld = "delete from medusa_content_evaluation_today_recommend_each_video_play_info where day=?"
            util.delete(deleteOld,insertDate)
          }
          playMergerRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1,programRedisObj.getTitleBySid(e._1),new JLong(e._2),new JLong(e._3))
          })

          filterRdd.unpersist()
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

  def getPathLength(str1:String,str2:String)={
    var res=false
    if(str1==null){
      if(str2!=null){
        if(str2.contains("home-hotrecommend")){
          val len=str2.split("-").length
          if(len==5 || len==4) res=true
        }
      }
    }else{
      if(str1.contains("home*recommendation")){
        val len=str1.split("\\*").length
        if(len==3) res=true
      }
    }
    res
  }
}
