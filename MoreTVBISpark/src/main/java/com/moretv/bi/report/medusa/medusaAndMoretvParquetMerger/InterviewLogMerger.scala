package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.PathParser
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/detail
  * input: /mbi/parquet/detail/$date
  * output: /log/medusaAndMoretv/parquet/$date/detail
  */
object InterviewLogMerger extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(InterviewLogMerger,args)
  }
   override def execute(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         sqlContext.udf.register("pathParser",PathParser.pathParser _)
         val logType = "interview"
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           val logDir1 = s"/log/medusa/parquet/$inputDate/$logType"
           val logDir2 = s"/mbi/parquet/$logType/$inputDate"
           val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$logType"

           val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",logType)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$logType",inputDate)
            if(p.deleteOld){
              HdfsUtil.deleteHDFSFile(outputPath)
            }

           if(medusaFlag && moretvFlag){
             val medusaDf = sqlContext.read.parquet(logDir1)
             val moretvDf = sqlContext.read.parquet(logDir2)
             val medusaColNames = medusaDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             val moretvColNames = moretvDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")


             val sqlSelectMedusa = s"select $medusaColNames,'medusa' as flag from log_data_1"
             val sqlSelectMoretv = s"select $moretvColNames,date as day, pathParser('interview',path," +
               "'path','contentType') as contentType,'moretv' as flag from log_data_2"

             val df1 = sqlContext.sql(sqlSelectMedusa).toJSON
             val df2 = sqlContext.sql(sqlSelectMoretv).toJSON

             val mergerDf = df1.union(df2)

             sqlContext.read.json(mergerDf).write.parquet(outputPath)
           }else if(!medusaFlag && moretvFlag){
             val moretvDf = sqlContext.read.parquet(logDir2)
             val moretvColNames = moretvDf.columns.toList.mkString(",")
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMoretv = s"select $moretvColNames,date as day, pathParser('interview',path," +
               "'path','contentType') as contentType,'moretv' as flag from log_data_2"
             sqlContext.sql(sqlSelectMoretv).write.parquet(outputPath)
           }else if(medusaFlag && !moretvFlag){
             val medusaDf = sqlContext.read.parquet(logDir1)
             val medusaColNames = medusaDf.columns.toList.mkString(",")
             medusaDf.registerTempTable("log_data_1")
             val sqlSelectMedusa = s"select $medusaColNames,'medusa' as flag from log_data_1"
             sqlContext.sql(sqlSelectMedusa).write.parquet(outputPath)
           }else{
             throw new RuntimeException("Do not exists files!")
           }


           cal.add(Calendar.DAY_OF_MONTH, -1)
         })

       }
       case None=>{throw new RuntimeException("At least needs one param: startDate!")}
     }
   }
 }
