package com.moretv.bi.temp

import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.HdfsUtil
import com.moretv.bi.util.ParamsParseUtil._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Will on 2016/6/3.
  * This program was to get a snapshot from db which can keep the status of db at the specific moment.
  */
object TencentVideoInfo extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    withParse(args) {
      p => {
        val s = sqlContext
        import s.implicits._

        val sqlMinMaxId1 = "select min(id),max(id) from mtv_basecontent"
        val sqlData1 = "select id,parent_id,sid,display_name,content_type,video_type from mtv_basecontent " +
          "where id >= ? and id <= ? and origin_status = 1 and status = 1 and " +
          "content_type in ('movie','tv','kids','comic','jilu','zongyi')"
        MySqlOps.getJdbcRDD(sc,DataBases.MORETV_CMS_MYSQL,sqlMinMaxId1,sqlData1,100,rs => {
          (rs.getLong(1),rs.getLong(2),rs.getString(3),rs.getString(4),rs.getString(5),rs.getInt(6))
        }).distinct().toDF("id","parent_id","sid","display_name","content_type","video_type").
          persist(StorageLevel.MEMORY_AND_DISK).
          registerTempTable("mtv_basecontent")

        val sqlMinMaxId2 = "select min(id),max(id) from mtv_media_file"
        val sqlData2 = "select content_id from mtv_media_file " +
          "where id >= ? and id <= ? and origin_status = 1 and status = 1 and " +
//          "source in ('tencent2','qq')"
          "source = 'tencent2'"
        MySqlOps.getJdbcRDD(sc,DataBases.MORETV_CMS_MYSQL,sqlMinMaxId2,sqlData2,100,rs => {
          rs.getLong(1)
        }).distinct().toDF("content_id").persist(StorageLevel.MEMORY_AND_DISK).
          registerTempTable("mtv_media_file")

        val df1 = sqlContext.sql("select a.* from mtv_basecontent a join mtv_media_file b on " +
          "a.id = b.content_id where a.video_type = 0")
        sqlContext.sql("select distinct a.parent_id as id from mtv_basecontent a join mtv_media_file b on " +
          "a.id = b.content_id where a.video_type = 2").registerTempTable("mtv_parent_id")
        val df2 = sqlContext.sql("select a.* from mtv_basecontent a join mtv_parent_id b on " +
          "a.id = b.id where a.video_type = 1").distinct()

        val outputPath = s"/log/temp/parquet/${p.startDate}/mtv_tencent"

        if (p.deleteOld) HdfsUtil.deleteHDFSFile(outputPath)

        df1.unionAll(df2).distinct().write.parquet(outputPath)
      }
    }
  }
}
