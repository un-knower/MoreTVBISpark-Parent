package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.UDFConstantDimension
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Created by wangbaozhi on 2016/12/26,
  * This object is used to generate dimension table and exchange the primary key from dimension table and fat table
  * input: /log/medusaAndMoretvMergerDimension/$date/playview2filter
  * output:/data_warehouse/dimensions/medusa/
  *
  * 1.generate dimension table from big fat table
  * 2.exchange
  *
  * take source_list（列表页分类入口） for example
  */
object PlayViewLogDimensionExchange extends BaseClass {
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "80")
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    println("-------------------------in execute--------------")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val startDate = p.startDate
        val inputDirFatTableBase = UDFConstantDimension.MEDUSA_BIG_FACT_TABLE_DIR
        val inputLogType = UDFConstantDimension.MEDUSA_BIG_FACT_TABLE_PLAY_TYPE
        val inputDataWarehouseDimensionsDir = UDFConstantDimension.MEDUSA_DIMENSION_DATA_WAREHOUSE
        val inputDimAppVersionDirName = UDFConstantDimension.DIM_APP_VERSION_TABLE_NAME

        val outputFactTableDirBase = UDFConstantDimension.MEDUSA_DATA_WAREHOUSE
        val outputFactTableType = UDFConstantDimension.FACT_MEDUSA_PLAY


        val outputDirDimensionBase = UDFConstantDimension.MEDUSA_DAILY_DIMENSION_DATA_WAREHOUSE
        val outputTypeSourceRetrieval = UDFConstantDimension.SOURCE_RETRIEVAL_TABLE
        val outputTypeSourceSearch = UDFConstantDimension.SOURCE_SEARCH_TABLE
        val outputTypeSourceList = UDFConstantDimension.SOURCE_LIST_TABLE
        val outputTypeSourceRecommend = UDFConstantDimension.SOURCE_RECOMMEND_TABLE
        val outputTypeSourceSpecial = UDFConstantDimension.SOURCE_SPECIAL_TABLE
        val outputTypeSourceLauncher = UDFConstantDimension.SOURCE_LAUNCHER_TABLE


        var printOutputMap: Map[String, String] = Map()

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputDirFatTable = s"$inputDirFatTableBase/$inputDate/$inputLogType"
          val inputDimAppVersionTable = s"$inputDataWarehouseDimensionsDir/$inputDimAppVersionDirName"
          val inputDirFatTableFlag = FilesInHDFS.IsInputGenerateSuccess(inputDirFatTable)
          val inputDimAppVersionTableFlag = FilesInHDFS.IsInputGenerateSuccess(inputDimAppVersionTable)
          printOutputMap+=("inputDate"->inputDate)
          printOutputMap+=("inputDirFatTable"->inputDirFatTable)
          printOutputMap+=("inputDimAppVersionTable"->inputDimAppVersionTable)
          printOutputMap+=("inputDirFatTableFlag"->inputDirFatTableFlag.toString)
          printOutputMap+=("inputDimAppVersionTableFlag"->inputDimAppVersionTableFlag.toString)


          val outputPath = s"$outputFactTableDirBase/$outputFactTableType/$inputDate"
          val outputPathSourceRetrieval = s"$outputDirDimensionBase/$inputDate/$outputTypeSourceRetrieval"
          val outputPathSourceSearch = s"$outputDirDimensionBase/$inputDate/$outputTypeSourceSearch"
          val outputPathSourceList = s"$outputDirDimensionBase/$inputDate/$outputTypeSourceList"
          val outputPathSourceRecommend = s"$outputDirDimensionBase/$inputDate/$outputTypeSourceRecommend"
          val outputPathSourceSpecial = s"$outputDirDimensionBase/$inputDate/$outputTypeSourceSpecial"
          val outputPathSourceLauncher = s"$outputDirDimensionBase/$inputDate/$outputTypeSourceLauncher"
          printOutputMap+=("outputPath"->outputPath)
          printOutputMap+=("outputPathSourceRetrieval"->outputPathSourceRetrieval)
          printOutputMap+=("outputPathSourceSearch"->outputPathSourceSearch)
          printOutputMap+=("outputPathSourceList"->outputPathSourceList)
          printOutputMap+=("outputPathSourceRecommend"->outputPathSourceRecommend)
          printOutputMap+=("outputPathSourceSpecial"->outputPathSourceSpecial)
          printOutputMap+=("outputPathSourceLauncher"->outputPathSourceLauncher)
          printOutputMap.keys.foreach { path =>
            println(s"$path = " + printOutputMap(path))
          }

          if (inputDirFatTableFlag && inputDimAppVersionTableFlag) {
            if (p.deleteOld) {
              HdfsUtil.deleteHDFSFile(outputPath)
              HdfsUtil.deleteHDFSFile(outputPathSourceRetrieval)
              HdfsUtil.deleteHDFSFile(outputPathSourceSearch)
              HdfsUtil.deleteHDFSFile(outputPathSourceList)
              HdfsUtil.deleteHDFSFile(outputPathSourceRecommend)
              HdfsUtil.deleteHDFSFile(outputPathSourceSpecial)
              HdfsUtil.deleteHDFSFile(outputPathSourceLauncher)
            }
              println("-------------------------in inputDirFatTableFlag  --------------")
              val dfFactTable = sqlContext.read.parquet(inputDirFatTable)
              val dfDimAppVersionTable = sqlContext.read.parquet(inputDimAppVersionTable)
              //println("dfFactTable.schema.fieldNames.mkString(\",\"):"+dfFactTable.schema.fieldNames.mkString(","))
              //println("dfFactTable.columns.toList.mkString(\",\"):"+dfFactTable.columns.toList.mkString(","))

              val sourceListMd=UDFConstantDimension.SOURCE_LIST_COLUMN
              val sourceListMdKey=UDFConstantDimension.SOURCE_LIST_SK
              val filterMd=UDFConstantDimension.SOURCE_RETRIEVAL_COLUMN
              val filterMdKey=UDFConstantDimension.SOURCE_RETRIEVAL_SK
              val searchMd=UDFConstantDimension.SOURCE_SEARCH_COLUMN
              val searchMdKey=UDFConstantDimension.SOURCE_SEARCH_SK
              val recommendMd=UDFConstantDimension.SOURCE_RECOMMEND_COLUMN
              val recommendKey=UDFConstantDimension.SOURCE_RECOMMEND_SK
              val specialMd=UDFConstantDimension.SOURCE_SPECIAL_COLUMN
              val specialKey=UDFConstantDimension.SOURCE_SPECIAL_SK
              val launcherMd=UDFConstantDimension.SOURCE_LAUNCHER_COLUMN
              val SOURCE_LAUNCHER_COLUMN_NOT_SHOW=UDFConstantDimension.SOURCE_LAUNCHER_COLUMN_NOT_SHOW
              val launcherKey=UDFConstantDimension.SOURCE_LAUNCHER_SK


            //需要通过多个字段关联，当满足条件，替换为md5 [dim_app_version]
            ///data_warehouse/dw_dimensions/dim_app_version
            val appVersionKey=UDFConstantDimension.DIM_APP_VERSION_KEY



            //生成事实表，去掉用来生成md5的列
            val fatTableColumnArray=dfFactTable.columns
            val fatTableColumns=fatTableColumnArray.map(e=>e.trim).mkString(",")

            val noNeedShowInFactString=s"$sourceListMd,$filterMd,$searchMd,$recommendMd,$specialMd,$SOURCE_LAUNCHER_COLUMN_NOT_SHOW"
            val noNeedShowInFactColumnArray=noNeedShowInFactString.split(',')
            val factColumnsArray=dfFactTable.columns.filter(e=>{!noNeedShowInFactColumnArray.contains(e)})
            val factColumnsString=factColumnsArray.mkString(",")
            println("noNeedShowInFactTable:"+noNeedShowInFactString)
            println("fatTableColumns:"+fatTableColumns)
            println("dfFactTable.columns.toList.size:"+dfFactTable.columns.toList.size)
            println("factColumnsArray.size:"+factColumnsArray.size)
            println("factColumnsString:"+factColumnsString)
            //

              //将大宽表进行维度替换，生成维度替换后的事实表
              dfFactTable.registerTempTable("log_data")
              dfDimAppVersionTable.registerTempTable("dim_app_version_table")
              val sqlSelectMedusa = s"select md5(concat($sourceListMd)) $sourceListMdKey,md5(concat($filterMd)) $filterMdKey,"+
                s"md5(concat($searchMd)) $searchMdKey,md5(concat($recommendMd)) $recommendKey,"+
                s"md5(concat($specialMd)) $specialKey,md5(concat($launcherMd)) $launcherKey,b.app_version_key as $appVersionKey,$factColumnsString "+
                " from log_data a "+
             " left outer join dim_app_version_table b "+
             " on  trim(if(a.buildDate is null,'',a.buildDate))=trim(if(b.build_time is null,'',b.build_time)) "+
             " and trim(if(a.apkVersion is null,'',a.apkVersion))=trim(if(b.version is null,'',b.version)) "+
             " and trim(if(a.apkSeries is null,'',a.apkSeries))=trim(if(b.app_series is null,'',b.app_series))"
            println("sqlSelectMedusa:"+sqlSelectMedusa)
              sqlContext.sql(sqlSelectMedusa).write.parquet(outputPath)

            //left outer join dim_app_version_table b  on trim(if(a.buildDate is null,'',a.buildDate))=trim(if(b.build_time is null,'',b.build_time)) and trim(a.apkVersion)=trim(b.version) and trim(a.apkSeries)=trim(b.app_series)

             //生成维度字典数据
             val sqlSelectSourceRetrieval = s"select distinct md5(concat($filterMd)) $filterMdKey,$filterMd from log_data "
             sqlContext.sql(sqlSelectSourceRetrieval).write.parquet(outputPathSourceRetrieval)
             val sqlSelectSourceSearch = s"select distinct md5(concat($searchMd)) $searchMdKey,$searchMd from log_data "
             sqlContext.sql(sqlSelectSourceSearch).write.parquet(outputPathSourceSearch)
             val sqlSelectSourceList = s"select distinct md5(concat($sourceListMd)) $sourceListMdKey,$sourceListMd from log_data "
             sqlContext.sql(sqlSelectSourceList).write.parquet(outputPathSourceList)
             val sqlSelectSourceRecommend = s"select distinct md5(concat($recommendMd)) $recommendKey,$recommendMd from log_data "
             sqlContext.sql(sqlSelectSourceRecommend).write.parquet(outputPathSourceRecommend)
             val sqlSelectSourceSpecial = s"select distinct md5(concat($specialMd)) $specialKey,$specialMd from log_data "
             sqlContext.sql(sqlSelectSourceSpecial).write.parquet(outputPathSourceSpecial)
             val sqlSelectSourceLauncher = s"select distinct md5(concat($launcherMd)) $launcherKey,$launcherMd from log_data "
             sqlContext.sql(sqlSelectSourceLauncher).write.parquet(outputPathSourceLauncher)
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


/*
*

cd /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin
sh /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin/submit.sh \
com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.PlayViewLogDimensionExchange --startDate 20161201






* */
