package com.moretv.bi.logprocess

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.constant.LogFields._
import com.moretv.bi.constant.LogType._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{HdfsUtil, ParamsParseUtil, SparkSetting, UserBlackListUtil}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.util.matching.Regex


object Log2ParquetNew extends BaseClass {
  val numCores = 220
  val outDir = "/mbi/parquet/"

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "15g").
      set("spark.cores.max", numCores.toString).
      set("spark.memory.storageFraction","0.3").
      set("spark.executor.cores","5").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator", "com.moretv.bi.apart.MyRegistrator").
      set("spark.speculation","true").
      set("spark.speculation.multiplier","1.4").
      set("spark.speculation.interval","1000").
      set("spark.scheduler.mode","FIFO")
    ModuleClass.executor(Log2ParquetNew,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val readFormat = new SimpleDateFormat("yyyyMMdd")
        val cal = Calendar.getInstance()
        cal.setTime(readFormat.parse(p.startDate))
        val logTypes = if(p.logTypes != "") p.logTypes.split(",") else Array[String]()

        val regex = if(logTypes.isEmpty) "log=(\\w{1,30})-".r
        else {
          val logTypeReg = p.logTypes.replace(',','|')
          s"log=($logTypeReg)-".r
        }

        (0 until p.numOfDays).foreach(i => {

          val logDate = readFormat.format(cal.getTime)
          val rawLogRdd = sc.textFile(s"/log/data/moretvlog.access.log-$logDate-*").
            filter(UserBlackListUtil.isNotBlack).
            map(log => matchLog(regex,log)).
            filter(_ != null).setName("raw").
            persist(StorageLevel.MEMORY_AND_DISK_SER)

          val logTypeSet = if(logTypes.isEmpty){
            rawLogRdd.map(_._1).distinct().collect()
          }else logTypes
        logTypeSet.foreach(logType => {
          try {

            val logRdd = rawLogRdd.filter(_._1 == logType).map(_._2)
            toParquet(logType, logDate, logRdd, p.deleteOld)
          }
          catch {
            case e: Exception =>
              println("====================================start==================================================")
              println(s"Trown exception when process [$logType] log.The detail of exception was shown as below.")
              println()
              e.printStackTrace()
              println("=====================================end=================================================")
          }})

          rawLogRdd.unpersist()
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }


    def matchLog(regex: Regex,log: String) = {
      regex findFirstMatchIn log match {
        case Some(m) => (m.group(1), log)
        case None => null
      }
    }


    def toParquet(logType: String, date: String, logRdd: RDD[String], deleteHDFS: Boolean)(implicit sqlContext: SQLContext) = {

      import sqlContext.implicits._

      val tmpData = logRdd.map(log => {
        try {
          val logData = LogPreProcess.matchLog(logType, log)
          val videoSid = logData.videoSid
          val contentType = logData.contentType
          //强制过滤掉tvn8p8t9km9v(脏数据)，编辑填错类型产生的。2016-08-04 连凯
          if(logType == "play" && videoSid == "tvn8p8t9km9v" && contentType == "movie") null else logData
        }
        catch {
          case e: NumberFormatException => null
        }
      }).filter(_ != null).toDF.
        persist(StorageLevel.MEMORY_AND_DISK_SER)
      val numPar = math.max(1,tmpData.count().toInt/4000000)
      val data = tmpData.repartition(numPar)
      logType match {
        case OPERATION => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$OPERATION_E/$date/")
            HdfsUtil.deleteHDFSFile(s"$outDir$OPERATION_ACW/$date/")
            HdfsUtil.deleteHDFSFile(s"$outDir$OPERATION_MM/$date/")
            HdfsUtil.deleteHDFSFile(s"$outDir$OPERATION_ST/$date/")
          }
          data.filter(s"logType = '$OPERATION_E'").select("logType", OPERATION_E_FIELDS: _*).
            write.save(s"$outDir$OPERATION_E/$date/")
          data.filter(s"logType = '$OPERATION_ACW'").select("logType", OPERATION_ACW_FIELDS: _*).
            write.save(s"$outDir$OPERATION_ACW/$date/")
          data.filter(s"logType = '$OPERATION_MM'").select("logType", OPERATION_MM_FIELDS: _*).
            write.save(s"$outDir$OPERATION_MM/$date/")
          data.filter(s"logType = '$OPERATION_ST'").select("logType", OPERATION_ST_FIELDS: _*).
            write.save(s"$outDir$OPERATION_ST/$date/")
        }
        case COLLECT => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$COLLECT/$date/")
          }
          data.filter(s"logType = '$COLLECT'").select("logType", COLLECT_FIELDS: _*).
            write.save(s"$outDir$COLLECT/$date/")
        }
        case LIVE => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$LIVE/$date/")
          }
          data.filter(s"logType = '$LIVE'").select("logType", LIVE_FIELDS: _*).
            write.save(s"$outDir$LIVE/$date/")
        }
        case DETAIL => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$DETAIL/$date/")
            HdfsUtil.deleteHDFSFile(s"$outDir$DETAIL_SUBJECT/$date/")
          }
          data.filter(s"logType = '$DETAIL'").select("logType", DETAIL_FIELDS: _*).
            write.save(s"$outDir$DETAIL/$date/")
          data.filter(s"logType = '$DETAIL_SUBJECT'").select("logType", DETAIL_SUBJECT_FIELDS: _*).
            write.save(s"$outDir$DETAIL_SUBJECT/$date/")
        }
        case PLAY => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$PLAYVIEW/$date/")
            HdfsUtil.deleteHDFSFile(s"$outDir$PLAYQOS/$date/")
            HdfsUtil.deleteHDFSFile(s"$outDir$PLAY_BAIDU_CLOUD/$date/")
          }
          data.filter(s"logType = '$PLAYVIEW'").select("logType", PLAYVIEW_FIELDS: _*).
            write.save(s"$outDir$PLAYVIEW/$date/")
          data.filter(s"logType = '$PLAYQOS'").select("logType", PLAYQOS_FIELDS: _*).
            write.save(s"$outDir$PLAYQOS/$date/")
          data.filter(s"logType = '$PLAY_BAIDU_CLOUD'").select("logType", PLAY_BAIDU_CLOUD_FIELDS: _*).
            write.save(s"$outDir$PLAY_BAIDU_CLOUD/$date/")
        }
        case INTERVIEW => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$INTERVIEW/$date/")
          }
          data.filter(s"logType = '$INTERVIEW'").select("logType", INTERVIEW_FIELDS: _*).
            write.save(s"$outDir$INTERVIEW/$date/")
        }
        case APPRECOMMEND => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$APPRECOMMEND/$date/")
          }
          data.filter(s"logType = '$APPRECOMMEND'").select("logType", APPRECOMMEND_FIELDS: _*).
            write.save(s"$outDir$APPRECOMMEND/$date/")
        }
        case ENTER => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$ENTER/$date/")
          }
          data.filter(s"logType = '$ENTER'").select("logType", ENTER_FIELDS: _*).
            write.save(s"$outDir$ENTER/$date/")
        }
        case EXIT => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$EXIT/$date/")
          }
          data.filter(s"logType = '$EXIT'").select("logType", EXIT_FIELDS: _*).
            write.save(s"$outDir$EXIT/$date/")
        }
        case HOMEACCESS => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$HOMEACCESS/$date/")
          }
          data.filter(s"logType = '$HOMEACCESS'").select("logType", HOMEACCESS_FIELDS: _*).
            write.save(s"$outDir$HOMEACCESS/$date/")
        }
        case MTV_ACCOUNT => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$MTV_ACCOUNT/$date/")
          }
          data.filter(s"logType = '$MTV_ACCOUNT'").select("logType", MTV_ACCOUNT_FIELDS: _*).
            write.save(s"$outDir$MTV_ACCOUNT/$date/")
        }
        case PAGEVIEW => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$PAGEVIEW/$date/")
          }
          data.filter(s"logType = '$PAGEVIEW'").select("logType", PAGEVIEW_FIELDS: _*).
            write.save(s"$outDir$PAGEVIEW/$date/")
        }
        case POSITION_ACCESS => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$POSITION_ACCESS/$date/")
          }
          data.filter(s"logType = '$POSITION_ACCESS'").select("logType", POSITION_ACCESS_FIELDS: _*).
            write.save(s"$outDir$POSITION_ACCESS/$date/")
        }
        case APPSUBJECT => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$APPSUBJECT/$date/")
          }
          data.filter(s"logType = '$APPSUBJECT'").select("logType", APPSUBJECT_FIELDS: _*).
            write.save(s"$outDir$APPSUBJECT/$date/")
        }
        case STAR_PAGE => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$STAR_PAGE/$date/")
          }
          data.filter(s"logType = '$STAR_PAGE'").select("logType", STAR_PAGE_FIELDS: _*).
            write.save(s"$outDir$STAR_PAGE/$date/")
        }
        case RETRIEVAL => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$RETRIEVAL/$date/")
          }
          data.filter(s"logType = '$RETRIEVAL'").select("logType", RETRIEVAL_FIELDS: _*).
            write.save(s"$outDir$RETRIEVAL/$date/")
        }
        case SET => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$SET/$date/")
          }
          data.filter(s"logType = '$SET'").select("logType", SET_FIELDS: _*).
            write.save(s"$outDir$SET/$date/")
        }
        case HOME_RECOMMEND => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$HOME_RECOMMEND/$date/")
          }
          data.filter(s"logType = '$HOME_RECOMMEND'").select("logType", HOME_RECOMMEND_FIELDS: _*).
            write.save(s"$outDir$HOME_RECOMMEND/$date/")
        }
        case HOME_VIEW => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$HOME_VIEW/$date/")
          }
          data.filter(s"logType = '$HOME_VIEW'").select("logType", HOME_VIEW_FIELDS: _*).
            write.save(s"$outDir$HOME_VIEW/$date/")
        }
        case DANMU_STATUS => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$DANMU_STATUS/$date/")
          }
          data.filter(s"logType = '$DANMU_STATUS'").select("logType", DANMU_STATUS_FIELDS: _*).
            write.save(s"$outDir$DANMU_STATUS/$date/")
        }
        case DANMU_SWITCH => {
          if (deleteHDFS) {
            HdfsUtil.deleteHDFSFile(s"$outDir$DANMU_SWITCH/$date/")
          }
          data.filter(s"logType = '$DANMU_SWITCH'").select("logType", DANMU_SWITCH_FIELDS: _*).
            write.save(s"$outDir$DANMU_SWITCH/$date/")
        }
      }
      if (deleteHDFS) {
        HdfsUtil.deleteHDFSFile(s"$outDir$CORRUPT/$logType/$date/")
      }
      data.filter(s"logType = '$CORRUPT'").select("logType", "rawLog").rdd.coalesce(5).
        saveAsTextFile(s"$outDir$CORRUPT/$logType/$date/", classOf[BZip2Codec])

      tmpData.unpersist()
    }
  }
}













