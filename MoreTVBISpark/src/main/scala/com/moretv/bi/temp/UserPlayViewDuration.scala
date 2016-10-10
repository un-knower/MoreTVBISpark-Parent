package com.moretv.bi.temp

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import com.moretv.bi.util.FileUtils._


/**
 * Created by laishun on 15/10/8.
 */
object UserPlayViewDuration extends SparkSetting{

  def main(args: Array[String]) {
    config.setAppName("UserPlayViewDuration")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    val path = "/mbi/parquet/playview/{"+"20151013,"+"20151014,"+"20151015,"+"20151016,"+"20151017,"+"20151018,"+"20151019"+"}/*"
    val df = sqlContext.read.load(path).persist(StorageLevel.MEMORY_AND_DISK)
    val zongyiResult = df.filter("path like '%home-hotrecommend-11-0-zongyi33%'").select("datetime","userId").
                       map(e=>(timeConvert(e(0).toString),e(1))).map(e =>(e._1,1)).reduceByKey((x,y)=>x+y).sortByKey().collect()
    val hotResult = df.filter("path like '%home-hotrecommend-0-0-hot11%'").select("datetime","userId").
                       map(e=>(timeConvert(e(0).toString),e(1))).map(e =>(e._1,1)).reduceByKey((x,y)=>x+y).sortByKey().collect()

    withCsvWriter("zongyi.txt")(out=>{
        zongyiResult.foreach(x =>{
            out.println(x._1._1+"  "+x._1._2+"  "+x._2)
        })
    })

    withCsvWriter("hot.txt")(out=>{
      hotResult.foreach(x =>{
        out.println(x._1._1+"  "+x._1._2+"  "+x._2)
      })
    })


  }

  def timeConvert(time:String)={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val formatCN = new SimpleDateFormat("yyyy-MM-dd")
    val cal =Calendar.getInstance()
    cal.setTime(format.parse(time))
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    (formatCN.format(cal.getTime),hour)
  }
}
