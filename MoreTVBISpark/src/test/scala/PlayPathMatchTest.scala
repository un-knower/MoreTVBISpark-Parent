
import org.junit.Test

/**
  * Created by witnes on 10/18/16.
  */
class PlayPathMatchTest {

  @Test
  def mvPathTst = {
    import com.moretv.bi.report.medusa.channeAndPrograma.mv.PlayPathMatch

    val event = "startplay"

    val userId = "apple"

    val duration = 1000L

    val strLst = List(
      //榜单
      "home*classification*mv-mv*mvTopHomePage*meiguogonggaopai_2016_42",
      "home*classification*mv-mv*mvTopHomePage*site_mvtop-mv_poster",
      "home*classification*mv-mv*mvTopHomePage*rege_2016_291",
      "home*classification*mv-mv*mvTopHomePage*xinge_2016_291",
      //我的-收藏
      "home*classification*mv-mv*mineHomePage*site_collect-mv_collection",
      //电台
      "home*classification*mv-mv*mvRecommendHomePage*8qoqt96kwx3e-mv_station",
      //推荐
      "home*classification*mv-mv*mvRecommendHomePage*abuw3fkmqsqr",
      //分类 -【风格，地区，年代】
      "home*classification*mv-mv*mvCategoryHomePage*site_mvstyle-mv_category*流行",
      //入口-搜索
      "home*classification*mv-mv-search*DSKL",
      //入口-舞蹈
      "home*classification*mv-mv*function*site_dance-mv_category*宅舞",
      //入口-演唱会
      "home*classification*mv-mv*function*site_concert-mv_category*华语",
      //入口-精选集
      "home*classification*mv-mv*function*site_mvsubject-mv_poster",
      //入口-歌手
      "home*classification*mv-mv*function*site_hotsinger-mv_poster"
    )

    strLst.foreach(w => {

      println(w)
      val list = PlayPathMatch.mvPathMatch(w, event, userId, duration)

      list.foreach(e => {
        println(e._1, e._2, e._3, e._4, e._5)
      })
    })


  }
}
