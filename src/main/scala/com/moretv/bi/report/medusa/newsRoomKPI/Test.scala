package com.moretv.bi.report.medusa.newsRoomKPI

import com.moretv.bi.util.ParquetSchema

/**
  * Created by baozhiwang on 2017/4/1.
  */
object Test extends App{
  val a=scala.collection.immutable.List("accessArea","accessLocation")
  /*for (i <- a){
    val j="a."+i
    println(j)
  }*/

    val bb=a.filter(e => {
    ParquetSchema.schemaArr.contains(e)
  }).map(e=>{
    "a."+e
  }).mkString(",")
  println(bb)
  /*private val regex ="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r
  val path="home-sports-arrange_cba-1_sport_group_beijing"
   regex findFirstMatchIn "sports1" match {
    case Some(p) => {
      println(p.group(1))
    }
    case None => {
     println("not match")
    }
  }*/

}
