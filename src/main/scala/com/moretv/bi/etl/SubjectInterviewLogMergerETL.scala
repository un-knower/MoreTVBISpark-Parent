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
        //引入专题维度表
        val dimension_subject_input_dir = DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT)
        val dimensionSubjectFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_subject_input_dir)
        println(s"--------------------dimensionSubjectFlag is ${dimensionSubjectFlag}")
        if (dimensionSubjectFlag) {
          val dimension_subject_df = DataIO.getDataFrameOps.getDimensionDF(sqlContext,p.paramMap,MEDUSA_DIMENSION,DimensionTypes.DIM_MEDUSA_SUBJECT)
          dimension_subject_df.registerTempTable(DimensionTypes.DIM_MEDUSA_SUBJECT)
        } else {
          throw new RuntimeException(s"--------------------dimension subject not exist")
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
            medusaDf.cache()
            moretvDf.cache()
            medusaDf.registerTempTable("medusa_table")
            moretvDf.registerTempTable("moretv_table")

            val medusaColNames = medusaDf.columns.toList.filter(e => {
              ParquetSchema.schemaArr.contains(e)
            }).mkString(",")
            val moretvColNames = moretvDf.columns.toList.filter(e => {
              ParquetSchema.schemaArr.contains(e)
            }).mkString(",")

            sqlStr =
              s"""
                |select $medusaColNames,
                |'medusa' as flag
                |from medusa_table
              """.stripMargin
            val medusaDfSelect = sqlContext.sql(sqlStr).toJSON

            sqlStr =
              s"""
                |select $moretvColNames,
                |'moretv' flag
                |from moretv_table
              """.stripMargin
            val moretvDfSelect = sqlContext.sql(sqlStr).toJSON

            //merge medusa and moretv
            val mergerRdd = medusaDfSelect.union(moretvDfSelect)
            if (p.deleteOld) {
              HdfsUtil.deleteHDFSFile(outputPath)
            }
            val mergeredDf = sqlContext.read.json(mergerRdd).toDF()
            mergeredDf.cache()
            mergeredDf.registerTempTable("mergered_df_table")
            val mergerColNamesWithTable = mergeredDf.columns.toList.filter(e => {
              ParquetSchema.schemaArr.contains(e)
            }).map(e => {
              "a." + e
            }).mkString(",")

            sqlStr =
              s"""
                 |select $mergerColNamesWithTable,
                 |b.subject_name subjectTitle
                 |from mergered_df_table a
                 |left join
                 |${DimensionTypes.DIM_MEDUSA_SUBJECT} b
                 |on trim(a.subjectCode)=trim(b.subject_code)
                     """.stripMargin
            val resultDf = sqlContext.sql(sqlStr)
            resultDf.write.parquet(outputPath)
          } else if (!medusaFlag && moretvFlag) {
            val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MORETV, LogTypes.DETAIL_SUBJECT, inputDate)
            moretvDf.cache()
            moretvDf.registerTempTable("moretv_table")
            val moretvColNames = moretvDf.columns.toList.map(e=> {
              "a." + e
            }).mkString(",")

            sqlStr =
              s"""
                |select $moretvColNames,
                |date as day,
                |b.subject_name as subjectTitle,
                |'moretv' flag
                |from moretv_table a
                |left join
                |${DimensionTypes.DIM_MEDUSA_SUBJECT} b
                |on trim(a.subject_code)=trim(b.subject_code)
              """.stripMargin
            sqlContext.sql(sqlStr).write.parquet(outputPath)
          } else if (medusaFlag && !moretvFlag) {
            val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.SUBJECT, inputDate)
            medusaDf.cache()
            medusaDf.registerTempTable("medusa_table")
            val medusaColNames = medusaDf.columns.toList.map(e=>{
              "a." + e
            }).mkString(",")

            sqlStr =
              s"""
                |select $medusaColNames,
                |b.subject_name as subjectTitle,
                |'medusa' as flag
                |from medusa_table a
                |left join
                |${DimensionTypes.DIM_MEDUSA_SUBJECT} b
                |on trim(a.subject_code)=trim(b.subject_code)
              """.stripMargin
            sqlContext.sql(sqlStr).write.parquet(outputPath)
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
