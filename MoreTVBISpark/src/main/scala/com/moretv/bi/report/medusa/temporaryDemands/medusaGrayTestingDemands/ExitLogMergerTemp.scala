package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util.{ParquetSchema, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/exit
  * input: /mbi/parquet/exit/$date
  * output: /log/medusaAndMoretv/parquet/$date/exit
  */
object ExitLogMergerTemp extends SparkSetting{
   def main(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         val sc = new SparkContext(config)
         val sqlContext = new SQLContext(sc)
         val logType = "exit"
         val cal = Calendar.getInstance()
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           val logDir1 = s"$medusaDir/$inputDate/$logType"
           val logDir2 = s"$moretvDir/$logType/$inputDate"
           val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",logType)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$logType",inputDate)
           val outputPath = s"/log/medusaAndMoretvMerger/temp/$inputDate/$logType"

           if(medusaFlag && moretvFlag) {

             val medusaDf = sqlContext.read.parquet(logDir1)
             val moretvDf = sqlContext.read.parquet(logDir2)
//             val medusaColNames = medusaDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
//             val moretvColNames = moretvDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             val medusaColNames = medusaDf.columns.toList.mkString(",")
             val moretvColNames = moretvDf.columns.toList.mkString(",")
             //       注册临时表
             medusaDf.registerTempTable("log_data_1")
//             moretvDf.selectExpr(ParquetSchema.schemaTypeConvert(moretvDf.columns):_*).registerTempTable("log_data_2")
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMedusa = s"select $medusaColNames,'medusa' as flag from log_data_1"
             val sqlSelectMoretv = s"select $moretvColNames,'moretv' as flag from log_data_2"

             val newDf1 = sqlContext.sql(sqlSelectMedusa)
             val newDf2 = sqlContext.sql(sqlSelectMoretv)
             val df1 = newDf1.toJSON
             val df2 = newDf2.toJSON
             val mergerDf = df1.union(df2)
             sqlContext.read.json(mergerDf).write.parquet(outputPath)
           }
           cal.add(Calendar.DAY_OF_MONTH, -1)
         })

       }
       case None=>{throw new RuntimeException("At least needs one param: startDate!")}
     }
   }
 }
