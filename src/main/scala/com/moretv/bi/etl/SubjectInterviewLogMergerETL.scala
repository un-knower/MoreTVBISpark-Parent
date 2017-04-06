package com.moretv.bi.etl

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DimensionTypes, LogTypes}
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel


/**
  * Created by jiulin.wu on 2017/4/6.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/detail-subject
  * input: /mbi/parquet/interview/$date
  * output: /log/medusaAndMoretv/parquet/$date/subject_interview
  */
object SubjectInterviewLogMergerETL extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //引入维度表
        val dimension_subject_input_dir = DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT)
        val dimensionSubjectFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_subject_input_dir)
        println(s"--------------------dimensionSubjectFlag is ${dimensionSubjectFlag}")
        if (dimensionSubjectFlag) {
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT).registerTempTable(DimensionTypes.DIM_MEDUSA_SUBJECT)
        } else {
          throw new RuntimeException(s"--------------------dimension not exist")
        }

        var sqlStr: String = ""
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.SUBJECT, inputDate)
          val moretv_input_dir = DataIO.getDataFrameOps.getPath(MORETV, LogTypes.DETAIL_SUBJECT, inputDate)
          val outputPath = DataIO.getDataFrameOps.getPath(MERGER, LogTypes.SUBJECT_INTERVIEW_ETL, inputDate)
          val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
          val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)

          if (medusaFlag && moretvFlag) {
            val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.SUBJECT, inputDate)
            val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MORETV, LogTypes.DETAIL_SUBJECT, inputDate)
            val medusaColNames = medusaDf.columns.toList.filter(e => {
              ParquetSchema.schemaArr.contains(e)
            }).mkString(",")
            val moretvColNames = moretvDf.columns.toList.filter(e => {
              ParquetSchema.schemaArr.contains(e)
            }).mkString(",")
            medusaDf.cache()
            moretvDf.cache()
            medusaDf.registerTempTable("log_data_1")
            moretvDf.registerTempTable("log_data_2")

            /* val sqlSelectMedusa = s"select $medusaColNames,getSubjectName(subjectCode) as subjectTitle,'medusa' as " +
               "flag from log_data_1"
             val sqlSelectMoretv = s"select $moretvColNames,date as day,getSubjectName(subjectCode) as subjectTitle," +
               s"'moretv' as flag  from log_data_2"*/

            val sqlSelectMedusa = s"select $medusaColNames,'medusa' as " +
              "flag from log_data_1"
            val sqlSelectMoretv = s"select $moretvColNames,date as day," +
              s"'moretv' as flag  from log_data_2"

            val df1 = sqlContext.sql(sqlSelectMedusa).toJSON
            val df2 = sqlContext.sql(sqlSelectMoretv).toJSON
            val mergerRdd = df1.union(df2)
            if (p.deleteOld) {
              HdfsUtil.deleteHDFSFile(outputPath)
            }
            val mergered_df = sqlContext.read.json(mergerRdd).toDF()
            mergered_df.cache()
            mergered_df.registerTempTable("mergered_df_table")
            val mergerColNamesWithTable = mergered_df.columns.toList.filter(e => {
              ParquetSchema.schemaArr.contains(e)
            }).map(e => {
              "a." + e
            }).mkString(",")
            sqlStr =
              s"""
                         select  $mergerColNamesWithTable,
                 |       b.subject_name,
                 |from mergered_df_table a
                 |left join
                 |    ${DimensionTypes.DIM_MEDUSA_SUBJECT} b
                 |on trim(a.subject_code)=trim(b.subject_code)
                     """.stripMargin
            val result_df = sqlContext.sql(sqlStr)
            result_df.write.parquet(outputPath)
          } else if (!medusaFlag && moretvFlag) {
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MORETV, LogTypes.DETAIL_SUBJECT, inputDate).repartition(24).persist(StorageLevel.MEMORY_AND_DISK)

            val moretvColNames = moretvDf.columns.toList.mkString(",")
            moretvDf.registerTempTable("log_data_2")
            val sqlSelectMoretv = s"select $moretvColNames,date as day,getSubjectName(subjectCode) as subjectTitle," +
              s"'moretv' as flag " +
              " from log_data_2"
            sqlContext.sql(sqlSelectMoretv).write.parquet(outputPath)
          } else if (medusaFlag && !moretvFlag) {
            //val medusaDf = sqlContext.read.parquet(logDir1)
            val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.SUBJECT, inputDate).repartition(24).persist(StorageLevel.MEMORY_AND_DISK)
            val medusaColNames = medusaDf.columns.toList.mkString(",")
            medusaDf.registerTempTable("log_data_1")
            val sqlSelectMedusa = s"$medusaColNames,getSubjectName(subjectCode) as subjectTitle,'medusa' as flag " +
              " from log_data_1"
            sqlContext.sql(sqlSelectMedusa).write.parquet(outputPath)
          }
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })


      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
