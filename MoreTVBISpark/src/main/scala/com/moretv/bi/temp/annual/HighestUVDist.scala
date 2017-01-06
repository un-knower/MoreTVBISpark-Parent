package com.moretv.bi.temp.annual

import java.lang.{Long => JLong}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by witnes on 1/3/17.
  */
object HighestUVDist extends BaseClass {

  private val tableName = "uv_minutes_dist"

  private val insertSql = s"insert into $tableName(minuteId,uv)values(?,?)"

  private val limit: Long = 60

  val zipDiff = (z1: Long, z2: Long) => z1 - z2

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        val q = sqlContext
        import q.implicits._


        val df = sqlContext.read.parquet(p.srcPath)
          .orderBy($"minuteId".asc)
          .map(e => (e.getString(1), e.getLong(0)))
          .groupByKey.map(e => (e._1, pairMaker(e._2)))
          .toDF("userId", "minutes")
          .withColumn("minutes", explode($"minutes"))
          .withColumn("minuteId", explode($"minutes"))
          .drop($"minutes")
          .groupBy("minuteId")
          .agg(countDistinct("userId").as("uv"))

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)


        df.collect.foreach(w => {
          util.insert(insertSql, new JLong(w.getLong(0)), new JLong(w.getLong(1)))
        })


      }
      case None => {

      }

    }


  }


  /**
    * 转化成 分钟时间片
    *
    * @param iter
    * @return
    */
  def pairMaker(iter: Iterable[Long]): Seq[Seq[Long]] = {

    val mirror = iter.toArray


    val diffPieces = mirror :+ mirror.last zip mirror.head +: mirror map {
      case (z1, z2) => zipDiff(z1, z2)
    }

    /** 前后两个触发时间大于60min 的截断点索引号 **/
    val interceptPoints = diffPieces.map(_ <= limit)
      .zipWithIndex
      .filter(!_._1)
      .map(_._2)

    val startPoints = 0 +: interceptPoints
    val endPoints = interceptPoints :+ mirror.lastIndexOf(mirror.last)

    startPoints zip endPoints map {
      case (e1, e2) => {
        val start = mirror(e1)

        val end = if (e2 > 0) mirror(e2 - 1) + 1 else e1
        Seq.range(start, end)
      }
    }

  }


}


