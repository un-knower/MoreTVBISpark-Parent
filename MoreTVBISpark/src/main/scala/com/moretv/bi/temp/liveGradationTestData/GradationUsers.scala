package com.moretv.bi.temp.liveGradationTestData

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.{HdfsUtil, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/10/11.
 * 计算小鹰直播10月8号和9号升级至3.1.1版本的用户
 */
object GradationUsers extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val outputHDFS = "/log/medusa/temple/gradationUsers"
        val parentDir = "/log/dbsnapshot/parquet/"
        val logType = "moretv_mtv_account"
        val date1 = "20161007/"
        val date2 = "2016100{8,9}/"
        val oldInputDir = s"${parentDir}${date1}${logType}"
        val newInputDir = s"${parentDir}${date2}${logType}"
        val oldUserDF = sqlContext.read.parquet(oldInputDir).select("user_id","current_version").
          filter("current_version != 'MoreTV_TVApp3.0_Medusa_3.1.1'").select("user_id")
        val newUserDF = sqlContext.read.parquet(newInputDir).select("user_id","current_version").
          filter("current_version = 'MoreTV_TVApp3.0_Medusa_3.1.1'").select("user_id")
        val updateUserDF = (oldUserDF.intersect(newUserDF)).toDF("userId")
        if(p.deleteOld){
          HdfsUtil.deleteHDFSFile(outputHDFS)
        }
        updateUserDF.write.parquet(outputHDFS)
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
