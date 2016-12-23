package com.moretv.bi.apart


import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{HdfsUtil, SparkSetting}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

/**
 * Created by Will on 2015/8/10.
 */
object LogApart extends BaseClass{
  val numCores = 220
  private val regex = "log=(\\w{1,30})-".r

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "30g").
      set("spark.cores.max", numCores.toString).
      set("spark.memory.storageFraction","0.3").
      set("spark.executor.cores","10").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator", "com.moretv.bi.apart.MyRegistrator").
      set("spark.speculation","true").
      set("spark.speculation.multiplier","1.4").
      set("spark.speculation.interval","1000").
      set("spark.scheduler.mode","FIFO")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)

    /*val logRdd = sc.textFile(inputPath).
      repartition(numCores*5).
      map(matchLog).
      filter(_ != null).setName("raw").
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    val logTypeSet = logRdd.map(_._1).distinct().collect().par
    logTypeSet.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(5))

    logTypeSet.foreach(logType => {
      val tmp = logRdd.filter(_._1 == logType).
        map(_._2).setName(logType).
        persist(StorageLevel.MEMORY_AND_DISK_SER)
      val n = tmp.count()
      val nPartitions = Math.max(1, (n/2100000).toInt)

      val path = outputPath+logType
      HdfsUtil.deleteHDFSFile(path)
      tmp.coalesce(nPartitions,true).
        saveAsTextFile(path,classOf[BZip2Codec])
      tmp.unpersist()
    })
    logRdd.unpersist()*/


    /*val logRdd = sc.textFile(inputPath).
      repartition(5*numCores).
      mapPartitions(iter=>{
        iter.map(matchLog).
          filter(_ != null).
          toArray.
          groupBy(x=>x._1).
          map(x=>(x._1,x._2.map(_._2))).
          toIterator
      }).setName("raw").
      persist(StorageLevel.MEMORY_AND_DISK_SER)
    val logTypeSet = logRdd.map(_._1).distinct().collect().par
    logTypeSet.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(2))
    logTypeSet.foreach(logType=>{
      val tmp = logRdd.
        filter(_._1==logType).
        flatMap(_._2).
        setName(logType).
        persist(StorageLevel.MEMORY_AND_DISK_SER)
      val n = tmp.count()
      val nPartitions = Math.max(1, (n/2100000).toInt)
      val path = outputPath+logType
      HdfsUtil.deleteHDFSFile(path)
      tmp.coalesce(nPartitions,true).
        saveAsTextFile(path,classOf[BZip2Codec])
      tmp.unpersist()
    })*/

    /*val logRdd = sc.textFile(inputPath).
      repartition(5*numCores).
      mapPartitions(iter=>{
        iter.map(matchLog).
          filter(_ != null).
          toArray.
          groupBy(x=>x._1).
          map(x=>(x._1,x._2.map(_._2))).
          toIterator
      }).setName("raw").
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    val logTypeSet = logRdd.map(_._1).distinct().
      collect().par
    logTypeSet.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(4))
    val rdds = logTypeSet.map(logType=>{
        val tmp = logRdd.
          filter(_._1==logType).
          flatMap(_._2).
          setName(logType).
          persist(StorageLevel.MEMORY_AND_DISK_SER)
        val n = tmp.count()
        val nPartitions = Math.max(1, (n/2100000).toInt)
        val path = outputPath+logType
        (outputPath,nPartitions,tmp)
      }).
      par

    logRdd.unpersist()
//    rdds.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(30))

    rdds.foreach(x=>{
      HdfsUtil.deleteHDFSFile(x._1)
      x._3.coalesce(x._2,true).saveAsTextFile(x._1,classOf[BZip2Codec])
      x._3.unpersist()
    })*/


    val numPerSlice = 100
    val numEachPar = 2000000/numPerSlice

    val logRdd = sc.textFile(inputPath).
      repartition(5*numCores).
      mapPartitions(iter=>{
        iter.map(matchLog).
          filter(_ != null).
          toArray.
          groupBy(x=>x._1).
          map(x=>(x._1,x._2.map(_._2).sliding(numPerSlice,numPerSlice).map(_.reduce((x,y)=>x+"\n"+y)))).
          toIterator
      }).setName("raw").
      persist(StorageLevel.MEMORY_AND_DISK_SER)
    val logTypeSet = logRdd.map(_._1).distinct().collect().par
    logTypeSet.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(4))
    logTypeSet.foreach(logType=>{
      val tmp = logRdd.
        filter(_._1==logType).
        flatMap(_._2).
        setName(logType).
        persist(StorageLevel.MEMORY_AND_DISK_SER)
      val n = tmp.count().toInt
      val nPartitions = Math.max(1, n/numEachPar+1)
      val path = outputPath+logType
      HdfsUtil.deleteHDFSFile(path)
      tmp.coalesce(nPartitions,true).
        saveAsTextFile(path,classOf[BZip2Codec])
      tmp.unpersist()
    })


    /*val logRdd = sc.textFile(inputPath).
      repartition(5*numCores).
      mapPartitions(iter=>{
        iter.map(matchLog).
          filter(_ != null).
          toArray.
          groupBy(x=>x._1).
          map(x=>(x._1,x._2.map(_._2).sliding(numPerSlice,numPerSlice).map(_.reduce((x,y)=>x+"\n"+y)))).
          toIterator
      }).setName("raw").
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    val logTypeSet = logRdd.map(_._1).distinct().
      collect().par
    logTypeSet.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(2))
    val rdds = logTypeSet.map(logType=>{
        val tmp = logRdd.
          filter(_._1==logType).
          flatMap(_._2).
          setName(logType).
          persist(StorageLevel.MEMORY_AND_DISK_SER)
        val n = tmp.count()
        val nPartitions = Math.max(1, (n/numEachPar).toInt)
        val path = outputPath+logType
        (outputPath,nPartitions,tmp)
      }).
      par

    logRdd.unpersist()
    rdds.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(30))
    rdds.foreach(x=>{
      HdfsUtil.deleteHDFSFile(x._1)
      x._3.coalesce(x._2,true).saveAsTextFile(x._1,classOf[BZip2Codec])
      x._3.unpersist()
    })*/


  }

  def matchLog(log:String) = {
    regex findFirstMatchIn log match {
      case Some(m) => (m.group(1),log)
      case None => null
    }
  }
}
