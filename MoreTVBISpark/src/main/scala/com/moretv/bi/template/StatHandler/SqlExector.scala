package com.moretv.bi.template.StatHandler

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by witnes on 11/3/16.
  */
trait SqlExector {


  def search(sqlBeer:String)(implicit sqlContext:SQLContext): DataFrame = {

    sqlContext.sql(sqlBeer)
  }

}
