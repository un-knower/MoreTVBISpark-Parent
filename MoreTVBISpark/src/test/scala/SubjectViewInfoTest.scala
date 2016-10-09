package scala

import com.moretv.bi.report.medusa.contentEvaluation.QuerySubjectViewInfo._
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util.{SparkSetting, SubjectUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/9/30.
 */
object SubjectViewInfoTest {

  def main(args: Array[String]) {
    val config = new SparkConf().
      set("spark.master","local").
      setAppName("SubjectViewInfoTest")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)


    val files = FilesInHDFS.getFileFromHDFS(s"F:///project/MoreTVBISpark/src/test/res")
    for(fileName <- files.map(f=>f.getPath.toString)){
      println(fileName)
      executeComp(sqlContext,fileName)
    }
  }


  def executeComp(sqlContext:SQLContext,playviewInput:String) = {
      val sqlDF=sqlContext.read.parquet(playviewInput)
    sqlDF.printSchema()
    println(sqlDF.count())
    System.exit(0)
    sqlDF.show(10)
    System.exit(0)
        val sqlDF2=sqlDF.select("userId","launcherAreaFromPath","launcherAccessLocationFromPath",
        "pageDetailInfoFromPath","pathIdentificationFromPath","path"
          ,"pathPropertyFromPath","flag","event"
        )
         // .filter("userId is not null").
        //filter("(flag='medusa' and pathPropertyFromPath='subject') or flag='moretv'")
    sqlDF2.show(10)
    System.exit(0)
    sqlDF2.registerTempTable("log_data")


        val df=sqlContext.sql("select userId,launcherAreaFromPath,launcherAccessLocationFromPath," +
        "pageDetailInfoFromPath,pathIdentificationFromPath,path,pathPropertyFromPath,flag from log_data where " +
        "event='view'")
    val rdd = df.map(e=>{
        require(e != null)
        (e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),
          e.getString(5),e.getString(6),e.getString(7))
      }).repartition(28).cache()

      val medusaInfoRdd=rdd.filter(_._8=="medusa").map(e=>(e._1,e._2,e._3,e._4,e._5))
      val formattedMedusaRdd=medusaInfoRdd.map(e=>(getMedusaFormattedInfo(e._2,e._3,e._4,e._5),e._1))

      val moretvInfoRdd=rdd.filter(_._8=="moretv").map(e=>(e._1,e._6))
      val formattedMoretvRdd=moretvInfoRdd.flatMap(e=>(SubjectUtils.getSubjectCodeAndPathWithId(e._2,e._1)))
        .map(e=>((e._1._1,changeSourceNameToChinese(e._1._2)),e._2))
      val mergerInfoRdd=formattedMedusaRdd.union(formattedMoretvRdd).cache()
      mergerInfoRdd.map(e=>(s"${e._1._1}")).collect()

  }

}
