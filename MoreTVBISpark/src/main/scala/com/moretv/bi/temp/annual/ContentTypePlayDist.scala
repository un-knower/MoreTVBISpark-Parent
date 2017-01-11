package com.moretv.bi.temp.annual

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._


/**
  * Created by witnes on 1/10/17.
  */
object ContentTypePlayDist extends BaseClass {


  def main(args: Array[String]) {

    ModuleClass.executor(this, args)

  }

  override def execute(args: Array[String]): Unit = {


    val q = sqlContext
    import q.implicits._

    val pathBeforeMarch =
      "/mbi/parquet/playview/{201601*,201602*,2016030*,2016031*,20160320,20160321}"
    val pathAfterMarch =
      "/log/medusaAndMoretvMerger/{2016*,20170101}/playview"


    sqlContext.read.parquet(pathBeforeMarch :: pathAfterMarch :: Nil: _*)
      .select($"contentType", $"userId")

  }
}
