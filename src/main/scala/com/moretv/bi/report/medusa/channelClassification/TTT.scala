package com.moretv.bi.report.medusa.channelClassification

import com.moretv.bi.util.baseclasee.{LogConfig, BaseClass}

/**
  * Created by baozhiwang on 2017/3/31.
  */
object TTT extends LogConfig{
  private val channel_to_mysql_table=Map(
    CHANNEL_COMIC->"medusa_channel_eachtab_play_comic_info",
    CHANNEL_MOVIE->"medusa_channel_eachtab_play_movie_info",
    CHANNEL_TV->"medusa_channel_eachtab_play_tv_info",
    CHANNEL_HOT->"medusa_channel_eachtab_play_hot_info",
    CHANNEL_VARIETY_PROGRAM->"medusa_channel_eachtab_play_zongyi_info",
    CHANNEL_OPERA->"medusa_channel_eachtab_play_xiqu_info",
    CHANNEL_RECORD->"medusa_channel_eachtab_play_jilu_info")

  def main(args: Array[String]) {
    val tableName=channel_to_mysql_table.get(CHANNEL_COMIC).get
    println(tableName)

  }
}
