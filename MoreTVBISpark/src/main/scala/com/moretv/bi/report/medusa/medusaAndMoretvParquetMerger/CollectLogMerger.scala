package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.InfoTransform
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{HdfsUtil, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/detail
  * input: /mbi/parquet/detail/$date
  * output: /log/medusaAndMoretv/parquet/$date/detail
  */
object CollectLogMerger extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
   override def execute(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
       /*  val logType = "collect"
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"*/
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           /*val logDir1 = s"$medusaDir/$inputDate/$logType"
           val logDir2 = s"$moretvDir/$logType/$inputDate"
           val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$logType"
           val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",logType)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$logType",inputDate)*/


           val medusa_input_dir= DataIO.getDataFrameOps.getPath(MEDUSA,LogTypes.COLLECT,inputDate)
           val moretv_input_dir= DataIO.getDataFrameOps.getPath(MORETV,LogTypes.COLLECT,inputDate)
           val outputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.COLLECT,inputDate)
           val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
           val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)

           if(p.deleteOld){
             HdfsUtil.deleteHDFSFile(outputPath)
           }
           if(medusaFlag && moretvFlag){
             /*val medusaDf = sqlContext.read.parquet(logDir1)
             val moretvDf = sqlContext.read.parquet(logDir2)*/

             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.COLLECT,inputDate)
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.COLLECT,inputDate)

             val medusaColNames = medusaDf.columns.toList.mkString(",")
             val moretvColNames = moretvDf.columns.toList.mkString(",")
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")

             val sqlSelectMedusa = s"select $medusaColNames,'medusa' as flag from log_data_1"
             val sqlSelectMoretv = s"select $moretvColNames, date as day,'moretv' as flag from log_data_2"

             val json1 = sqlContext.sql(sqlSelectMedusa).toJSON
             val json2 = sqlContext.sql(sqlSelectMoretv).toJSON
             val mergerDf = json1.union(json2)
             sqlContext.read.json(mergerDf).write.parquet(outputPath)

           }else if(!medusaFlag && moretvFlag){
            // val moretvDf = sqlContext.read.parquet(logDir2)
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.COLLECT,inputDate)

             val moretvColNames = moretvDf.columns.toList.mkString(",")
             moretvDf.registerTempTable("log_data_2")

             val sqlSelectMoretv = s"select $moretvColNames, date as day,'moretv' as flag from log_data_2"
             sqlContext.sql(sqlSelectMoretv).write.parquet(outputPath)
           }else if(medusaFlag && !moretvFlag){
             //val medusaDf = sqlContext.read.parquet(logDir1)
             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.COLLECT,inputDate)

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
