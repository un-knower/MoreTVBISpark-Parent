package com.moretv.bi.operation

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/3/1.
 */
object DanmuSubmitPVUV extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(DanmuSubmitPVUV,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match{
      case Some(p)=>
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val path = "/log/danmu/parquet/"+date+"/bulletscreen/*"
          val df = sqlContext.read.parquet(path).filter("event = 'success'").select("wechatId").cache()
          val pv = df.count()
          val uv = df.distinct().count()

          if(p.deleteOld){
            val sqlDelete = "Delete from danmuSubmitPVUV where day = ?"
            util.delete(sqlDelete,day)
          }
          val sqlInsert = "INSERT INTO danmuSubmitPVUV(day,submit_num, user_num) VALUES(?,?,?)"
          util.insert(sqlInsert,day,new JLong(pv),new JLong(uv))
          df.unpersist()
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
        util.destory()
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }
}
