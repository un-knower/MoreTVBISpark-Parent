package com.moretv.bi.common

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/4/18.
 */
object AppRecommendInstall extends BaseClass{

  def main(args: Array[String]) {
    config.setAppName("AppRecommendInstall")
    ModuleClass.executor(AppRecommendInstall,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val path = "/mbi/parquet/apprecommend/"+p.startDate+"/part-*"
        val cacheValue = sqlContext.read.parquet(path).filter("event='install'").select("date","appSid","subjectCode","userId").map(e=>((e.getString(0),e.getString(1),e.getString(2)),e.getString(3))).countByKey()
        var sql = ""
        val dbUtil = new DBOperationUtils("bi")
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from appRecommendInstall where day = '$date'"
          dbUtil.delete(oldSql)
        }
        cacheValue.foreach(x =>{
            sql = "insert into appRecommendInstall(day,appCode,appName,subjectCode,subjectName,downloadNum) values(?,?,?,?,?,?)"
            var subject = x._1._3
            var subjectName = "null"
            if (subject == null || "".equals(subject)) {
              subject = "null"
            }else{
              if(subject.indexOf("app")==0 && subject.length > 3) {
                subjectName = CodeToNameUtils.getSubjectNameBySid(subject)
              }else{
                subject = "null"
              }
            }
            dbUtil.insert(sql,x._1._1,x._1._2,CodeToNameUtils.getApplicationNameBySid(x._1._2),subject,CodeToNameUtils.getSubjectNameBySid(subject),new Integer(x._2.toInt))
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
