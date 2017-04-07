package com.moretv.bi.etl

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.LogTypes
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by guohao on 2017/4/6.
  * 2.x moretv日志，通过path获取contentType
  * 2.x 3.x interview 日志合并，通过path
  */
object InterviewLogMergerETL extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
   override def execute(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         sqlContext.udf.register("getContentTypeByPath",getContentTypeByPath _)
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)

           val medusa_input_dir= DataIO.getDataFrameOps.getPath(MEDUSA,LogTypes.INTERVIEW,inputDate)
           val moretv_input_dir= DataIO.getDataFrameOps.getPath(MORETV,LogTypes.INTERVIEW,inputDate)
           val outputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.INTERVIEW_ETL,inputDate)
           val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
           val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)


            if(p.deleteOld){
              HdfsUtil.deleteHDFSFile(outputPath)
            }
           if(medusaFlag && moretvFlag){
             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.INTERVIEW,inputDate)
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.INTERVIEW,inputDate)

             val medusaColNames = medusaDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             val moretvColNames = moretvDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")


             val sqlSelectMedusa = s"select $medusaColNames,'medusa' as flag from log_data_1"
             val sqlSelectMoretv = s"select $moretvColNames,date as day, getContentTypeByPath(path) as contentType,'moretv' as flag from log_data_2"

             val df1 = sqlContext.sql(sqlSelectMedusa).toJSON
             val df2 = sqlContext.sql(sqlSelectMoretv).toJSON

             val mergerDf = df1.union(df2)

             sqlContext.read.json(mergerDf).write.parquet(outputPath)
           }else if(!medusaFlag && moretvFlag){
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.INTERVIEW,inputDate)
             val moretvColNames = moretvDf.columns.toList.mkString(",")
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMoretv = s"select $moretvColNames,date as day, getContentTypeByPath(path) as contentType,'moretv' as flag from log_data_2"
             sqlContext.sql(sqlSelectMoretv).write.parquet(outputPath)
           }else if(medusaFlag && !moretvFlag){

             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.INTERVIEW,inputDate)
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

  /**
    * 通过path 获取contentType
    * @param path
    * @return
    */
  def getContentTypeByPath(path:String) ={
    var contentType:String = null
    if (path != null) {
      if (path.contains("-")) {
        if (path.split("-").length >= 2) {
          contentType = path.split("-")(1)
          if("kids_home".equalsIgnoreCase(contentType)){
            contentType="kids"
          }
        }
      }
    }
    contentType
  /*  try {
      contentType = path.split("-")(1)
    }catch {
      case e:Exception=>{
          println(s"path is ${path} 不符合格式")
      }
    }
    contentType
    */

  }

 }
