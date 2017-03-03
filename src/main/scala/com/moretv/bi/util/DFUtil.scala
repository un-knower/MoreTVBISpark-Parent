package com.moretv.bi.util

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import tachyon.TachyonURI
import tachyon.client.TachyonFS

/**
 * Created by Will on 2015/8/13.
 * The utility of get dataframe by conditions.
 */
object DFUtil extends SparkSetting{

  private val dateFormat = new SimpleDateFormat("yyyyMMdd")

  /**
   * To get dataframe by date.The default params was to get yesterday's data.
   * @param logType the log type of log.
   * @param startDate the last of the period you want to get.
   * @param numOfDays the days you want to process
   * @param mode To get data from which fs.
   * @param sqlContext implicit parameter
   * @return dataframe
   */
  def getDFByDate(logType:String, startDate:String, numOfDays:Int=1,
                  mode:String = "hdfs")(implicit sqlContext:SQLContext) = {
    require(Set("tachyon","hdfs").contains(mode))
    val calendar = Calendar.getInstance()
    calendar.setTime(dateFormat.parse(startDate))
    val inputs = new Array[String](numOfDays)

    mode match {
      case "tachyon" =>
        val tfs = TachyonFS.get(new TachyonURI(tachyonMaster))
        for(i<- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          val tachyonDir = s"$tachyonMaster/tachyon/mbi/parquet/$logType/$date/"
          if(tfs.exist(new TachyonURI(tachyonDir))){
            inputs(i) = tachyonDir
          }else{
            inputs(i) = s"/mbi/parquet/$logType/$date/"
          }
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }

      case "hdfs" =>
        for(i<- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"/mbi/parquet/$logType/$date/"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
    }
    sqlContext.read.parquet(inputs:_*)
  }

  /**
   * @param sql to get the specific dataframe by sql
   * @param logType the log type of log.
   * @param startDate the last of the period you want to get.
   * @param numOfDays the days you want to process
   * @param mode To get data from which fs.
   * @param sqlContext implicit parameter
   * @return dataframe
   */
  def getDFByDateWithSql(sql:String, logType:String, startDate:String, numOfDays:Int=1,
                         mode:String = "hdfs")(implicit sqlContext:SQLContext) = {
    
    val df = getDFByDate(logType,startDate,numOfDays,mode)(sqlContext)

    //The dbOperation table name was log_data.So the sql should be written like 'select ... from log_data where ...'.
    df.registerTempTable("log_data")
    sqlContext.sql(sql)
  }


  /**
    * To get dataframe by date.The default params was to get yesterday's data.
    * @param logType the log type of log.
    * @param startDate the last of the period you want to get.
    * @param numOfDays the days you want to process
    * @param mode To get data from which fs.
    * @param sqlContext implicit parameter
    * @return dataframe
    */
  def getDFByDateV3(logType:String, startDate:String, numOfDays:Int=1,sqlContext:SQLContext,
                  mode:String = "hdfs") = {
    require(Set("tachyon","hdfs").contains(mode))
    val calendar = Calendar.getInstance()
    calendar.setTime(dateFormat.parse(startDate))
    val inputs = new Array[String](numOfDays)

    mode match {
      case "tachyon" =>
        val tfs = TachyonFS.get(new TachyonURI(tachyonMaster))
        for(i<- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          val tachyonDir = s"$tachyonMaster/tachyon/mbi/parquet/$logType/$date/"
          if(tfs.exist(new TachyonURI(tachyonDir))){
            inputs(i) = tachyonDir
          }else{
            inputs(i) = s"/log/medusaAndMoretvMerger/$date/$logType/"
          }
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }

      case "hdfs" =>
        for(i<- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          println(s"-----load file: /log/medusaAndMoretvMerger/$date/$logType/")
          inputs(i) = s"/log/medusaAndMoretvMerger/$date/$logType/"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
    }
    sqlContext.read.load(inputs:_*)
  }

}
