package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.PathParser
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel


/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/detail-subject
  * input: /mbi/parquet/interview/$date
  * output: /log/medusaAndMoretv/parquet/$date/subject_interview
  */
object SubjectInterviewLogMerger extends BaseClass{

  def main(args: Array[String]) {
    config.set("spark.eventLog.enabled","true").
      set("spark.eventLog.dir","hdfs://hans/xiajun/spark-events").
      set("spark.speculation","true").
      set("spark.speculation.interval", "100").
      set("spark.speculation.quantile","0.75").
      set("spark.speculation.multiplier","1.5")
    ModuleClass.executor(this,args)
  }
   override def execute(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         sqlContext.udf.register("getSubjectName",CodeToNameUtils.getSubjectNameBySid _)
         val startDate = p.startDate
         /*val medusaLogType = "subject"
         val moretvLogType = "detail-subject"
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"
         val outputLogType = "subject-interview"*/
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
     /*      val logDir1 = s"/log/medusa/parquet/$inputDate/$medusaLogType"
           val logDir2 = s"/mbi/parquet/$moretvLogType/$inputDate"
           val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",medusaLogType)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$moretvLogType",inputDate)
           val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$outputLogType"*/

           val medusa_input_dir= DataIO.getDataFrameOps.getPath(MEDUSA,LogTypes.SUBJECT,inputDate)
           val moretv_input_dir= DataIO.getDataFrameOps.getPath(MORETV,LogTypes.DETAIL_SUBJECT,inputDate)
           val outputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.SUBJECT_INTERVIEW,inputDate)
           val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
           val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)

           if(medusaFlag && moretvFlag){
             //val medusaDf = sqlContext.read.parquet(logDir1).repartition(24).persist(StorageLevel.MEMORY_AND_DISK)
             //val moretvDf = sqlContext.read.parquet(logDir2).repartition(24).persist(StorageLevel.MEMORY_AND_DISK)
             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.SUBJECT,inputDate).repartition(24).persist(StorageLevel.MEMORY_AND_DISK)
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.DETAIL_SUBJECT,inputDate).repartition(24).persist(StorageLevel.MEMORY_AND_DISK)
             val medusaColNames = medusaDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             val moretvColNames = moretvDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")

             val sqlSelectMedusa = s"select $medusaColNames,getSubjectName(subjectCode) as subjectTitle,'medusa' as " +
               "flag from log_data_1"
             val sqlSelectMoretv = s"select $moretvColNames,date as day,getSubjectName(subjectCode) as subjectTitle," +
               s"'moretv' as flag  from log_data_2"

             val df1 = sqlContext.sql(sqlSelectMedusa).toJSON
             val df2 = sqlContext.sql(sqlSelectMoretv).toJSON
             val mergerDf = df1.union(df2)
             if(p.deleteOld){
               HdfsUtil.deleteHDFSFile(outputPath)
             }
             sqlContext.read.json(mergerDf).write.parquet(outputPath)
             medusaDf.unpersist()
             moretvDf.unpersist()
           }else if(!medusaFlag && moretvFlag){
             //val moretvDf = sqlContext.read.parquet(logDir2)
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.DETAIL_SUBJECT,inputDate).repartition(24).persist(StorageLevel.MEMORY_AND_DISK)

             val moretvColNames = moretvDf.columns.toList.mkString(",")
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMoretv = s"select $moretvColNames,date as day,getSubjectName(subjectCode) as subjectTitle," +
               s"'moretv' as flag " +
               " from log_data_2"
             sqlContext.sql(sqlSelectMoretv).write.parquet(outputPath)
           }else if(medusaFlag && !moretvFlag){
             //val medusaDf = sqlContext.read.parquet(logDir1)
             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.SUBJECT,inputDate).repartition(24).persist(StorageLevel.MEMORY_AND_DISK)
             val medusaColNames = medusaDf.columns.toList.mkString(",")
             medusaDf.registerTempTable("log_data_1")
             val sqlSelectMedusa = s"$medusaColNames,getSubjectName(subjectCode) as subjectTitle,'medusa' as flag " +
               " from log_data_1"
             sqlContext.sql(sqlSelectMedusa).write.parquet(outputPath)
           }
           cal.add(Calendar.DAY_OF_MONTH, -1)
         })


       }
       case None=>{throw new RuntimeException("At least needs one param: startDate!")}
     }
   }
 }
