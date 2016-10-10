package com.moretv.bi.report

import java.io.PrintWriter

import com.moretv.bi.util.{ParamsParseUtil, ProductModelUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/5/8.
 * 用于统计当前数据库中用户的终端型号和终端品牌分布
 */
object LoginUserVersionDist extends SparkSetting{

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val inputDate = p.startDate
        sqlContext.read.load(s"/log/moretvloginlog/parquet/$inputDate/loginlog").registerTempTable("log_data")
        val versionMap = sqlContext.sql("select version,mac from log_data").
          map(row => {
            val version = row.getString(0)
            if(version != null){
              val idx = version.lastIndexOf("_")
              (version.substring(idx+1),row.getString(1))
            }else ("NULL",row.getString(1))
          }).filter(_ != null).distinct().countByKey()

        val file = s"/script/bi/moretv/liankai/file/VersionDist_$inputDate.csv"
        val out = new PrintWriter(file,"GBK")
        versionMap.foreach(
          e =>
            out.println(e._1+","+e._2)
        )
        out.close()

      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

}
