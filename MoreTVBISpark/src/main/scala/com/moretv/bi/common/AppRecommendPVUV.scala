package com.moretv.bi.common


import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object AppRecommendPVUV extends BaseClass{

  def main(args: Array[String]) {
    config.setAppName("AppRecommendPVUV")
    ModuleClass.executor(AppRecommendPVUV,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/apprecommend/"+p.startDate+"/part-*"
        val cacheValue = sqlContext.read.parquet(path).filter("event in ('view','install')").select("date","event","userId").
                         map(x => ((x.getString(0), x.getString(1)), x.getString(2))).persist()
        val userNumValue = cacheValue.distinct().countByKey()
        val accessNumValue = cacheValue.countByKey()
        val sql = "insert into appRecommendPVUV(day,type,user_num,user_access) values(?,?,?,?)"
        val dbUtil = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from appRecommendPVUV where day = '$date'"
          dbUtil.delete(oldSql)
        }
        userNumValue.foreach(
          x =>{
            dbUtil.insert(sql, x._1._1, x._1._2, new Integer(x._2.toInt),new Integer(accessNumValue.get(x._1).get.toInt))
          }
        )
        dbUtil.destory()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
}
