package com.moretv.bi.report.medusa.tempAnalysis

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/detail
  * input: /mbi/parquet/detail/$date
  * output: /log/medusaAndMoretv/parquet/$date/detail
  */
object parquetSchemaInfo extends SparkSetting{
   def main(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         val sc = new SparkContext(config)
         val sqlContext = new SQLContext(sc)
         val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           val insertDate = DateFormatUtils.toDateCN(inputDate,-1)
           val insertSql = "insert into medusa_moretv_parquet_columns(day, column_name) values (?,?)"
           val files = FilesInHDFS.getFileFromHDFS(s"$medusaDir/$inputDate")
           files.foreach(file=>{
             val fileName = file.getPath.getName
             val columns = sqlContext.read.parquet(s"$medusaDir/$inputDate/$fileName").columns
             columns.foreach(i=>{
               util.insert(insertSql,insertDate,i)
             })
           })
           val mtvFiles = FilesInHDFS.getFileFromHDFS(s"$moretvDir/")
           mtvFiles.foreach(file=>{
             val fileName = file.getPath.getName
             if(fileName!="_corrupt" && fileName!="appsubject"){
               val columns = sqlContext.read.parquet(s"$moretvDir/$fileName/$inputDate").columns
               columns.foreach(i=>{
                 util.insert(insertSql,insertDate,i)
               })
             }
           })
           cal.add(Calendar.DAY_OF_MONTH, -1)
         })

       }
       case None=>{throw new RuntimeException("At least needs one param: startDate!")}
     }
   }
 }
