package com.moretv.bi.ProgramViewAndPlayStats


import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object Channel_pv_uv extends BaseClass with DateUtil{
  def main(args: Array[String]): Unit = {
    config.setAppName("Channel_pv_uv")
    ModuleClass.executor(Channel_pv_uv,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        //calculate log whose type is play
        val path = "/mbi/parquet/detail/" + p.startDate + "/part-*"
        val df = sqlContext.read.load(path)
        val playRDD = df.select("date","path","userId").map(e => (e.getString(0), e.getString(1), e.getString(2))).
            map(e => (getKeys(e._1,e._2), e._3)).filter(_._1 != null).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = playRDD.distinct().countByKey()
        val accessNum = playRDD.countByKey()

        //save date
        val util = DataIO.getMySqlOps(DataBases.MORETV_EAGLETV_MYSQL)
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from channel_pv_uv where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO channel_pv_uv(year,month,day,weekstart_end,channel,uv_num,pv_num) VALUES(?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })

        playRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def getKeys(date:String, path:String)={
    //obtain time
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)

    val reg = "(home|thirdparty_\\d{1})-(comic|history|jilu|kids_home|kids|movie|search|tv|zongyi|hotrecommend|watchhistory|otherswatch|subject)".r
    val rel = reg findFirstMatchIn path
    val result = rel match {
      case Some(x)=>
        (year,month,date,week,x.group(2))
      case None => null
    }
    result
  }
}
