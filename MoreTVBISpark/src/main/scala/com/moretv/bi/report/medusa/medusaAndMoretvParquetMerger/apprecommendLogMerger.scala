package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * 合并moretv与medusa的apprecommend日志
  */
object apprecommendLogMerger extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(apprecommendLogMerger,args)
  }
   override def execute(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
      /* val logType = "apprecommend"
         val moretvType = "apprecommend"
         val medusaType = "appaccess"
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"*/
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           //val logDir1 = s"$medusaDir/$inputDate/$medusaType"
           val medusa_input_dir= DataIO.getDataFrameOps.getPath(MEDUSA,LogTypes.APPACCESS,inputDate)
           //val logDir2 = s"$moretvDir/$moretvType/$inputDate"
           val moretv_input_dir= DataIO.getDataFrameOps.getPath(MORETV,LogTypes.APP_RECOMMEND,inputDate)
           //val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$logType"
           val outputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.APP_RECOMMEND,inputDate)


          // val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",medusaType)
             val medusaFlag = FilesInHDFS.IsDirectoryExist(s"$medusa_input_dir")
           //val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$moretvType",inputDate)
             val moretvFlag = FilesInHDFS.IsDirectoryExist(s"$moretv_input_dir")

           if(p.deleteOld){
             HdfsUtil.deleteHDFSFile(outputPath)
           }
           if(medusaFlag && moretvFlag){
             //val medusaDf = sqlContext.read.parquet(logDir1)
             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.APPACCESS,inputDate)

            // val moretvDf = sqlContext.read.parquet(logDir2)
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.APP_RECOMMEND,inputDate)

             val meudsaColumnsName = medusaDf.columns.toList.mkString(",")
             val moretvColumnsName = moretvDf.columns.toList.mkString(",")
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")

             val sqlSelectMedusa = s"select $meudsaColumnsName,'medusa' as flag from log_data_1"
             val sqlSelectMoretv = s"select $moretvColumnsName,date as day,'moretv' as flag from log_data_2"
             val json1 = sqlContext.sql(sqlSelectMedusa).toJSON
             val json2 = sqlContext.sql(sqlSelectMoretv).toJSON
             val mergerJson = json1.union(json2)
             sqlContext.read.json(mergerJson).write.parquet(outputPath)
           }else if(!medusaFlag && moretvFlag){
             val moretvDf = sqlContext.read.parquet(s"$moretv_input_dir")
             val moretvColumnsName = moretvDf.columns.toList.mkString(",")
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMoretv = s"select $moretvColumnsName,date as day,'moretv' as flag from log_data_2"
             sqlContext.sql(sqlSelectMoretv).write.parquet(outputPath)
           }else if(medusaFlag && !moretvFlag){
             val medusaDf = sqlContext.read.parquet(s"$medusa_input_dir")
             val meudsaColumnsName = medusaDf.columns.toList.mkString(",")
             medusaDf.registerTempTable("log_data_1")
             val sqlSelectMedusa = s"select $meudsaColumnsName,'medusa' as flag from log_data_1"
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
