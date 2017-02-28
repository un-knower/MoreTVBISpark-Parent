package com.moretv.bi.logprocess

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{ HdfsUtil, SparkSetting}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser
import com.moretv.bi.constant.LogFields._
import com.moretv.bi.constant.LogType._


object Log2Parquet extends BaseClass{

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "60g").
      set("spark.cores.max", "220").
      set("spark.memory.storageFraction","0.3").
      set("spark.executor.cores","20")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]){

    val s = sqlContext
    import s.implicits._
    val readFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    var numOfDays = 1
    //需要转化的日志类型
    var calcType = Array(PLAY,DETAIL,COLLECT,OPERATION,LIVE,INTERVIEW,ENTER,EXIT,APPRECOMMEND,HOMEACCESS,MTV_ACCOUNT,
      PAGEVIEW,POSITION_ACCESS,APPSUBJECT,STAR_PAGE,RETRIEVAL,SET,HOME_RECOMMEND)
    var deleteHDFS = false
    val defaultParams = Params(
      1,
      readFormat.format(calendar.getTime),
      "all",
      false
    )

    val parser = new OptionParser[Params]("Log2Parquet") {
      head("Log2Parquet")
      opt[Int]("numOfDays").action((x,c)=>c.copy(numOfDays=x))
      opt[String]("startDate").action((x,c)=>c.copy(startDate=x))
      opt[String]("logTypes").action((x,c)=>c.copy(logTypes=x))
      opt[Boolean]("deleteHDFS").action((x,c)=>c.copy(delete = x))
    }

    var params:Params=null
    parser.parse(args,defaultParams) match {
      case Some(param)=>{
        params=param

        numOfDays = param.numOfDays
        calcType = if(param.logTypes == "all") calcType else param.logTypes.split(",")
        calendar.setTime(readFormat.parse(param.startDate))
        deleteHDFS = param.delete
      }
      case None =>{
        if(args.length==1){
          numOfDays = args(0).toInt
        }
        if(args.length==2){
          numOfDays = args(0).toInt
          calendar.setTime(readFormat.parse(args(1)))
        }

        if(args.length==3){
          numOfDays = args(0).toInt
          calendar.setTime(readFormat.parse(args(1)))
          calcType = args(2).split(",")
        }
      }
    }


    (0 until numOfDays).map(i=> {
      val logDate = readFormat.format(calendar.getTime)
      calendar.add(Calendar.DAY_OF_MONTH,-1)
      logDate
    }).foreach(logDate=>{
      calcType.foreach(logType=>{
        try {
          toParquet(logType, logDate, sc, deleteHDFS)
        }
        catch {
          case e:Exception =>
            println("======================================================================================")
            println(s"Trown exception when process $logType log.The detail of exception was shown as below.")
            println("======================================================================================")
            e.printStackTrace()
        }
      })})

  }


  def toParquet(logType:String,date:String,sc:SparkContext,deleteHDFS:Boolean)= {

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val readDir = s"/mbi/LogApart/$date/$logType"

    val data = sc.textFile(readDir).map(log => {
      try {
        LogPreProcess.matchLog(logType, log)
      }
      catch {
        case e:NumberFormatException => null
      }
    }).filter(_ != null).toDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    logType match {
      case OPERATION => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$OPERATION_E/$date/")
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$OPERATION_ACW/$date/")
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$OPERATION_MM/$date/")
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$OPERATION_ST/$date/")
        }
        data.filter(s"logType = '$OPERATION_E'").select("logType",OPERATION_E_FIELDS: _*).
          write.save(s"/mbi/parquet/$OPERATION_E/$date/")
        data.filter(s"logType = '$OPERATION_ACW'").select("logType",OPERATION_ACW_FIELDS: _*).
          write.save(s"/mbi/parquet/$OPERATION_ACW/$date/")
        data.filter(s"logType = '$OPERATION_MM'").select("logType",OPERATION_MM_FIELDS: _*).
          write.save(s"/mbi/parquet/$OPERATION_MM/$date/")
        data.filter(s"logType = '$OPERATION_ST'").select("logType",OPERATION_ST_FIELDS: _*).
          write.save(s"/mbi/parquet/$OPERATION_ST/$date/")
      }
      case COLLECT => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$COLLECT/$date/")
        }
        data.filter(s"logType = '$COLLECT'").select("logType",COLLECT_FIELDS: _*).
          write.save(s"/mbi/parquet/$COLLECT/$date/")
      }
      case LIVE => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$LIVE/$date/")
        }
        data.filter(s"logType = '$LIVE'").select("logType",LIVE_FIELDS: _*).
          write.save(s"/mbi/parquet/$LIVE/$date/")
      }
      case DETAIL => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$DETAIL/$date/")
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$DETAIL_SUBJECT/$date/")
        }
        data.filter(s"logType = '$DETAIL'").select("logType",DETAIL_FIELDS: _*).
          write.save(s"/mbi/parquet/$DETAIL/$date/")
        data.filter(s"logType = '$DETAIL_SUBJECT'").select("logType",DETAIL_SUBJECT_FIELDS: _*).
          write.save(s"/mbi/parquet/$DETAIL_SUBJECT/$date/")
      }
      case PLAY => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$PLAYVIEW/$date/")
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$PLAYQOS/$date/")
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$PLAY_BAIDU_CLOUD/$date/")
        }
        data.filter(s"logType = '$PLAYVIEW'").select("logType",PLAYVIEW_FIELDS: _*).
          write.save(s"/mbi/parquet/$PLAYVIEW/$date/")
        data.filter(s"logType = '$PLAYQOS'").select("logType",PLAYQOS_FIELDS: _*).
          write.save(s"/mbi/parquet/$PLAYQOS/$date/")
        data.filter(s"logType = '$PLAY_BAIDU_CLOUD'").select("logType",PLAY_BAIDU_CLOUD_FIELDS: _*).
          write.save(s"/mbi/parquet/$PLAY_BAIDU_CLOUD/$date/")
      }
      case INTERVIEW => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$INTERVIEW/$date/")
        }
        data.filter(s"logType = '$INTERVIEW'").select("logType",INTERVIEW_FIELDS: _*).
          write.save(s"/mbi/parquet/$INTERVIEW/$date/")
      }
      case APPRECOMMEND => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$APPRECOMMEND/$date/")
        }
        data.filter(s"logType = '$APPRECOMMEND'").select("logType",APPRECOMMEND_FIELDS: _*).
          write.save(s"/mbi/parquet/$APPRECOMMEND/$date/")
      }
      case ENTER => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$ENTER/$date/")
        }
        data.filter(s"logType = '$ENTER'").select("logType",ENTER_FIELDS: _*).
          write.save(s"/mbi/parquet/$ENTER/$date/")
      }
      case EXIT => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$EXIT/$date/")
        }
        data.filter(s"logType = '$EXIT'").select("logType",EXIT_FIELDS: _*).
          write.save(s"/mbi/parquet/$EXIT/$date/")
      }
      case HOMEACCESS => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$HOMEACCESS/$date/")
        }
        data.filter(s"logType = '$HOMEACCESS'").select("logType",HOMEACCESS_FIELDS: _*).
          write.save(s"/mbi/parquet/$HOMEACCESS/$date/")
      }
      case MTV_ACCOUNT => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$MTV_ACCOUNT/$date/")
        }
        data.filter(s"logType = '$MTV_ACCOUNT'").select("logType",MTV_ACCOUNT_FIELDS: _*).
          write.save(s"/mbi/parquet/$MTV_ACCOUNT/$date/")
      }
      case PAGEVIEW => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$PAGEVIEW/$date/")
        }
        data.filter(s"logType = '$PAGEVIEW'").select("logType",PAGEVIEW_FIELDS: _*).
          write.save(s"/mbi/parquet/$PAGEVIEW/$date/")
      }
      case POSITION_ACCESS => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$POSITION_ACCESS/$date/")
        }
        data.filter(s"logType = '$POSITION_ACCESS'").select("logType",POSITION_ACCESS_FIELDS: _*).
          write.save(s"/mbi/parquet/$POSITION_ACCESS/$date/")
      }
      case APPSUBJECT => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$APPSUBJECT/$date/")
        }
        data.filter(s"logType = '$APPSUBJECT'").select("logType",APPSUBJECT_FIELDS: _*).
          write.save(s"/mbi/parquet/$APPSUBJECT/$date/")
      }
      case STAR_PAGE => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$STAR_PAGE/$date/")
        }
        data.filter(s"logType = '$STAR_PAGE'").select("logType",STAR_PAGE_FIELDS: _*).
          write.save(s"/mbi/parquet/$STAR_PAGE/$date/")
      }
      case RETRIEVAL => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$RETRIEVAL/$date/")
        }
        data.filter(s"logType = '$RETRIEVAL'").select("logType",RETRIEVAL_FIELDS: _*).
          write.save(s"/mbi/parquet/$RETRIEVAL/$date/")
      }
      case SET => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$SET/$date/")
        }
        data.filter(s"logType = '$SET'").select("logType",SET_FIELDS: _*).
          write.save(s"/mbi/parquet/$SET/$date/")
      }
      case HOME_RECOMMEND => {
        if(deleteHDFS) {
          HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$HOME_RECOMMEND/$date/")
        }
        data.filter(s"logType = '$HOME_RECOMMEND'").select("logType",HOME_RECOMMEND_FIELDS: _*).
          write.save(s"/mbi/parquet/$HOME_RECOMMEND/$date/")
      }
    }
    if(deleteHDFS) {
      HdfsUtil.deleteHDFSFile(s"/mbi/parquet/$CORRUPT/$logType/$date/")
    }
    data.filter(s"logType = '$CORRUPT'").select("logType","rawLog").rdd.coalesce(5).
      saveAsTextFile(s"/mbi/parquet/$CORRUPT/$logType/$date/",classOf[BZip2Codec])

    data.unpersist()
  }
}













