package com.moretv.bi.template

import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by witnes on 11/15/16.
  */

/**
  * load - filter - save
  */
trait ParquetHandle {

  private val loadDate = ""

  private val loadPath = ""

  private val loadPaths = Seq.empty[String]

  private val savePath = ""

  private val savePaths = Seq.empty[String]

  def load()(filter: DataFrame => DataFrame)(implicit sqlContext: SQLContext) = {

    isMultiple(loadPath, loadPaths) match {
      case true => {
        val df = sqlContext.read.parquet(loadPaths: _*)
        filter(df)
      }
      case false => {
        val df = sqlContext.read.parquet(loadPath)
        filter(df)
      }
      case _ => {
        throw new Exception("out of control")
      }
    }

  }

  def save(df: DataFrame)(filter: DataFrame => DataFrame)(implicit sqlContext: SQLContext) = {

    isMultiple(savePath, savePaths) match {
      case true => {
        savePaths.foreach(path => {
          filter(df).write.parquet(path)
        })
      }
      case false => {
        filter(df).write.parquet(savePath)
      }
      case _ => {
        throw new Exception("out of control")
      }
    }

  }


  def isMultiple(param1: String, param2: Seq[String]): Boolean = {

    if (param1.equals("") && !loadPaths.isEmpty) {
      true
    }
    else if (loadPaths.isEmpty) {
      false
    }
    else {
      false
    }
  }

}
