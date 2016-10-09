package com.moretv.bi.common

import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import com.moretv.bi.util.SparkSetting
import org.apache.spark.sql.SQLContext
import java.lang.{Long => JLong}
import com.moretv.bi.util._
/**
 * Created by Administrator on 2015/12/15.
 */
object PeoplealsolikePVUV extends BaseClass{
  def main(args: Array[String]) {
    config.setAppName("PeoplealsolikePVUV")
    ModuleClass.executor(PeoplealsolikePVUV,args)
  }
 override def execute (args: Array[String]) {
   ParamsParseUtil.parse(args)match {
     case Some(p) => {
       val util = new DBOperationUtils("bi")
       val date = DateFormatUtils.toDateCN(p.startDate, -1)
       val sql = "select userId from log_data where path like '%peoplealsolike%'"
       val input = s"/mbi/parquet/detail/" + p.startDate + "/*"
       val df_init = sqlContext.read.parquet(input)
       df_init.registerTempTable("log_data")
       val df = sqlContext.sql(sql)
       val pv = df.count()
       val uv = df.distinct.count()

       if (p.deleteOld) {
         val sqlDelete = "DELETE FROM PeoplealsolikePVUV WHERE day = ?"
         util.delete(sqlDelete, date)
       }

       val sqlInsert = "INSERT INTO PeoplealsolikePVUV (day, user_num, user_access) VALUES(?,?,?)"
       util.insert(sqlInsert, date, new JLong(uv), new JLong(pv))

     }
     case None => {
       throw new RuntimeException("At least need param --excuteDate.")
     }
   }
 }
}
