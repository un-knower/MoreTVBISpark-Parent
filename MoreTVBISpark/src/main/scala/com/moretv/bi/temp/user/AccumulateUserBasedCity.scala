package src.com.moretv.bi.temp.user

import com.moretv.bi.util.FileUtils._
import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
//import com.moretv.bi.util.IPLocationUtils.IPLocationDataUtil
import  com.moretv.bi.util.IPUtils



/**
 * Created by zhangyu on 2016/7/22.
  * 统计时间区间内的累计用户数的城市分布
  * 程序中时间区间已固定，暂不修改。
 */

object AccumulateUserBasedCity extends SparkSetting {
  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    implicit val sqlContext = new SQLContext(sc)

    //sqlContext.udf.register("getCity",IPLocationDataUtil.getCity _)
    //sqlContext.udf.register("getProvince",IPLocationDataUtil.getProvince _)
    sqlContext.udf.register("getCity",(ip:String) => {
      val result = IPUtils.getProvinceAndCityByIp(ip)
      if(result != null) result(1) else "未知"
    })
    sqlContext.udf.register("getProvince",IPUtils.getProvinceByIp _)

//    println(IPLocationDataUtil.getProvince("1.12.16.231"))
//    println(IPLocationDataUtil.getCity("1.12.16.231"))
//    println(IPLocationDataUtil.ipLocationMap.size())

    sqlContext.read.load("/log/dbsnapshot/parquet/20160721/" +
      "moretv_mtv_account")
      .registerTempTable("log_data")

     val rdd = sqlContext.sql("select getProvince(ip),getCity(ip), count(distinct mac) from log_data " +
       "where openTime <= '2016-07-20 23:59:59' group by getProvince(ip),getCity(ip)").
       map(row =>((row.getString(0),row.getString(1)),row.getLong(2))).collectAsMap()
      val filepath = "/script/bi/moretv/zhangyu/file/allCityAccumulateuser.csv"

    withCsvWriterOld(filepath){
      out => {
        rdd.foreach(x => {
          out.println(x._1._1 + "," + x._1._2 + "," + x._2)
        })
      }
    }
  }
}



