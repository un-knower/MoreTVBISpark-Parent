import com.moretv.bi.report.medusa.liveCastStat
import org.junit.Test

/**
  * Created by witnes on 1/13/17.
  */
class LivePathMatchTest {

  import com.moretv.bi.report.medusa.liveCastStat.LiveSationTree

  @Test
  def categoryTest = {

    val strList = List(
      "home*live*game-webcast*电竞风",
      "home*live*life-webcast*看现场",
      "home*live*eagle-webcast*看现场",
      "home*live*eagle-webcast*潮娱乐"
    )

     strList.map(LiveSationTree.categoryMatch).foreach(println)


  }
}
