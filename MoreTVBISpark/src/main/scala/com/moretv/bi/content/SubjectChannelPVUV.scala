package com.moretv.bi.content

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.Calendar
import java.lang.{Long => JLong}
/**
 * Created by Administrator on 2016/3/1.
 */
object SubjectChannelPVUV extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(SubjectChannelPVUV,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match{
      case Some(p)=>
        val util = new DBOperationUtils("bi")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val pathInterview = "/mbi/parquet/interview/"+date+"/*"
          val pathPlayview = "/mbi/parquet/playview/"+date+"/*"
          val df_Interview = sqlContext.read.parquet(pathInterview).filter("event = 'enter' and path = 'home-subject'").select("userId").cache()
          val df_playview = sqlContext.read.parquet(pathPlayview).filter("event = 'playview' and path like 'home-subject%'").select("path","userId").cache()
          val pv = df_Interview.count()
          val uv = df_Interview.distinct().count()
          val playRdd = df_playview.map(row => {
                val path = row.getString(0)
                if(path.startsWith("home-subject") && !path.contains("-peoplealsolike") && !path.contains("-similar") &&
                  !path.contains("-actor") && !path.contains("-tag") && !path.contains("-mytag"))
                  (row.getString(1))
                else null
              }).filter(_!=null)
          val play_num = playRdd.count()
          val userPlay_num = playRdd.distinct().count()

          if(p.deleteOld){
            val sqlDelete = "Delete from SubjectChannelPVUVVV where day = ?"
            util.delete(sqlDelete,day)
          }
          val sqlInsert = "INSERT INTO SubjectChannelPVUVVV(day,user_num, access_num, userPlay_num, play_num) VALUES(?,?,?,?,?)"
          util.insert(sqlInsert,day,new JLong(uv),new JLong(pv),new JLong(userPlay_num),new JLong(play_num))
          df_Interview.unpersist()
          df_playview.unpersist()
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
        util.destory()
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }
}
