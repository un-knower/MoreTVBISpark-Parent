package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import com.moretv.bi.report.medusa.util.udf.{PathParserDimension, UDFConstantDimension}

/**
  * Created by baozhiwang on 2016/12/13.
  *
  * used in prod unit test,not delete
  */
object Test  {

  private val regex_tab = ("(kids_collect|kandonghua|tingerge|xuezhishi)\\*(观看历史|收藏追看|专题收藏|动画明星|热播推荐|" +
    "最新出炉|动画专题|欧美精选|国产精选|0-3岁启蒙|0-3岁|4-6岁|7-10岁|英文动画|少儿电影|儿童综艺|亲子交流|益智启蒙|童话故事|" +
    "教育课堂|随便听听|儿歌明星|儿歌热播|儿歌专辑|英文儿歌|舞蹈律动|热播推荐|4-7岁幼教|汉语学堂|英语花园|数学王国|安全走廊|" +
    "百科世界|艺术宫殿|搞笑|机战|亲子|探险)").r

  private val regex_search = ("(kids_collect-search|kandonghua-search|xuezhishi-search|" +
    "tingerge-search)\\*([0-9A-Za-z]+)").r


  private val regex_moretv_search = ("(.*-search-)([0-9A-Za-z]+)").r

  def matchRegex(str: String): String = {
    regex_search findFirstMatchIn str match {
      case Some(p) => {
        p.group(1) match {
          case "kandonghua-search" => "看动画" + "->" + "搜一搜"
          case "tingerge-search" => "听儿歌" + "->" + "搜一搜"
          case "xuezhishi-search" => "学知识" + "->" + "搜一搜"
          case _ => null
        }
      }
      case None => null
    }
  }

  def matchRegex2(str: String): String = {
    regex_moretv_search findFirstMatchIn str match {
      case Some(p) => p.group(2)
      case None => null
    }
  }

  def getPathMainInfo(x:String,index:Int,subIndex:Int)={
    var result:String = null
    //过滤null的信息
    if(x!=null){
      //判断是需要哪一级路径
      if(index>=2){
        //获取非第一级的信息
        if(x.contains("-")){
          val splitData = x.split("-")
          if(splitData.length>=index){
            val indexPath = splitData(index-1)
            result = getMedusaPathDetailInfo(indexPath,subIndex)
          }
        }
      }else{
        //直接获取第一级路径的信息
        if(x.contains("-")){
          if(x!="-"){
            val firstPath = x.split("-")(0)
            result = getMedusaPathDetailInfo(firstPath,subIndex)
          }
        }else{
          //只有一级路径
          result = getMedusaPathDetailInfo(x,subIndex)
        }
      }
    }
    result
  }
  def getMedusaPathDetailInfo(path:String,subIndex:Int)={
    var result:String = null

    if(path!=null) {
      if(subIndex>=2){
        /*所需信息为第一级路径中的其他字段信息*/
        if(path.contains("*")){
          if(path.split("\\*").length>=subIndex){
            result=path.split("\\*")(subIndex-1)
          }
        }
      }else{
        /*所需信息为第一级路径中的第一个字段信息*/
        if(path.contains("*")){
          if(path!="*"){
            try{
              result = path.split("\\*")(0)
            }catch{
              case e:Exception=>e.printStackTrace()
            }
          }
        }else{
          result = path.toString
        }
      }
    }
    result
  }

  def test(): Unit ={
    val mv_list=List("home*classification*mv-mv*电台*电台","home*classification*mv-mv*mvCategoryHomePage*site_mvstyle-mv_category*电子")
    val kids_list=List("home*classification*kids-kids_home-kandonghua*4-6岁","home*my_tv*kids-kids_home-kids_anim*英文动画","home*my_tv*kids-kids_home-kids_rhymes*儿歌热播*随便听听")
    val sport_list=List("home*classification*3-sports*League*ouguan-league*赛事回顾","home*my_tv*5-sports*horizontal*collect-sportcollection*比赛")
    val other_list=List("home*classification*tv-tv*电视剧专题","home*classification*movie-movie*动画电影","home*my_tv*jilu-jilu*前沿科技")
    val dirty_list=List("home*classification*comic-comic-retrieval*hot*huanxiang*all*all")

    val list = mv_list ++  kids_list ++  sport_list ++ other_list ++ dirty_list
    println(list.size)
    for (path <- list){
      val sub_path_1=  PathParserDimension.getListCategoryMedusa(path,1)
      val sub_path_2=  PathParserDimension.getListCategoryMedusa(path,2)
      println(s"path is $path,after parse,sub_path_1 is $sub_path_1,sub_path_2 is $sub_path_2")
    }
  }


  def test_shaixuan_v2: Unit ={
    val retrieval_list=List("home*classification*tv-tv-retrieval*hot*dushi*hanguo*all",
      "home*classification*tv-tv-retrieval*hot*dushi*hanguo*2000*2009","home*my_tv*movie-movie-retrieval*hot*kongbu*neidi*2016",
    "home*classification*movie-movie-retrieval*hot*all*all*qita")
    for (path <- retrieval_list){
      println(path)
      for( i <- 1 to 4){
        val result=PathParserDimension.getFilterCategory(path,UDFConstantDimension.RETRIEVAL_DIMENSION,i)
        println("index:"+i+",result:"+result)
    }
  }
  }


  def test_shaixuan_v3: Unit ={
    val multi_search_list=List("home-movie-multi_search-hot-kongbu-meiguo-all-peoplealsolike",
      "home-movie-multi_search-hot-kongbu-ouzhou-all-similar","home-movie-multi_search-hot-lishi-hanguo-all",
      "home-movie-multi_search-hot-kehuan-meiguo-2015",
      "home-movie-multi_search-hot-juqing-meiguo-2000-2009",
      "home-movie-multi_search-hot-xiju-neidi-qita",
      "home-mv-multi_search-hot-liuxing-neidi-2016","home-movie-multi_search-hot-kongbu-hanguo-2016"
    )
     for (path <- multi_search_list){
      println(path)
      for( i <- 1 to 4){
        val result=PathParserDimension.getFilterCategory(path,UDFConstantDimension.MULTI_SEARCH,i)
        println("index:"+i+",result:"+result)
      }
    }
  }


  //测试筛选功能
  def test_shaixuan: Unit ={
    //测试medusa
    val regex_medusa_filter = (".*retrieval\\*(hot|new|score)\\*([\\S]+)\\*([\\S]+)\\*(all|[0-9]+\\*[0-9]+)").r
    val retrieval_list=List("home*classification*tv-tv-retrieval*hot*dushi*hanguo*all","home*classification*tv-tv-retrieval*hot*dushi*hanguo*2000*2009")
    for (path <- retrieval_list){
      regex_medusa_filter findFirstMatchIn path match {
        case Some(p) => {
          println(path)
          println("1:"+p.group(1))
          println("2:"+p.group(2))
          println("3:"+p.group(3))
          println("4:"+p.group(4))
        }
        case None => null
      }
    }

    //测试moretv
    /*home-movie-multi_search-hot-kongbu-meiguo-all-peoplealsolike
home-movie-multi_search-hot-kongbu-ouzhou-all-similar
home-movie-multi_search-hot-lishi-hanguo-all
home-movie-multi_search-hot-kehuan-meiguo-2015           单个日期
home-movie-multi_search-hot-juqing-meiguo-2000-2009      最后年代是日期范围*/
    val regex_moretv_filter = (".*multi_search-(hot|new|score)-([\\S]+)-([\\S]+)-(all|[0-9]+-[0-9]+)").r
    val multi_search_list=List("home-movie-multi_search-hot-kongbu-meiguo-all-peoplealsolike",
      "home-movie-multi_search-hot-kongbu-ouzhou-all-similar","home-movie-multi_search-hot-lishi-hanguo-all",
      "home-movie-multi_search-hot-kehuan-meiguo-2015",
    "home-movie-multi_search-hot-juqing-meiguo-2000-2009",
    "home-movie-multi_search-hot-xiju-neidi-qita",
    "home-mv-multi_search-hot-liuxing-neidi-2016","home-movie-multi_search-hot-kongbu-hanguo-2016"
    )
    for (path <- multi_search_list){
      regex_moretv_filter findFirstMatchIn path match {
        case Some(p) => {
          println(path)
          println("1:"+p.group(1))
          println("2:"+p.group(2))
          println("3:"+p.group(3))
          println("4:"+p.group(4))
        }
        case None => null
      }
    }



  }



  def main(args: Array[String]): Unit = {
    val number_regex=("^\\d+$").r
     var result="91a"
    number_regex findFirstMatchIn result match {
      case Some(p) =>
      case None => result=null
    }
    println("----"+result)
    //test_shaixuan_v2
    //test

    //val regex_moretv_search = (".*retrieval\\*([\\S]+)\\*([\\S]+)\\*([\\S]+)\\*([\\S]+)").r

    System.exit(0)

    val path = "home*(classification*kids-kids_home-kids_anim*英文动画"

    val regex_medusa_list_category_kids = ("home\\*\\((my_tv|classification)\\*kids-kids_home-([0-9A-Za-z_]+)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r

    //val path:String ="home*classification*movie-movie*动画电影"
    regex_medusa_list_category_kids findFirstMatchIn path match {
      case Some(p) => {
        println("matched ...")
        println(p.group(1))
        println(p.group(2))
      }
      case None => null
    }


    /*  val path:String ="home*classification*3-sports*League*ouguan-league*赛事回顾"
    //val path:String = "home*classification*7-sports*League*ozb-league"
      val jianshen="瑜伽健身|情侣健身|增肌必备|快速燃脂"
      val dzjj="英雄联盟|穿越火线|王者荣耀|NEST"
      //val regex_medusa_list_category_sport =(s"home\\*classification\\*[0-9]+-sports\\*League\\*([dj|dzjj|CBA|ozb|ouguan|yc|jianshen|olympic]+)-league\\*([赛事回顾|热点新闻|精彩专栏|直播赛程|HPL|${jianshen}|${dzjj}]+)").r
      val regex_medusa_list_category_sport =(s"home\\*classification\\*[0-9]+-sports\\*League\\*([dj|dzjj|CBA|ozb|ouguan|yc|jianshen|olympic]+)-league\\*([赛事回顾|热点新闻|精彩专栏|直播赛程|HPL|${jianshen}|${dzjj}]+)").r

      regex_medusa_list_category_sport findFirstMatchIn  path match {
        case Some(p) => {
          println("matched ...")
          println(p.group(1))
          println(p.group(2))
        }
        case None => null
      }*/

    /*
         //val path:String ="home*classification*mv-mv*mvCategoryHomePage*site_mvstyle-mv_category*电子"
          //val path:String ="home*classification*mv-mv*电台*电台"
          var result=PathParserDimension.getListCategory(path,1)
          println(result)

            result=PathParserDimension.getListCategory(path,2)
          println(result)
      */

    /* val regex_medusa_list_category_mv =("home\\*classification\\*mv-mv\\*([a-zA-Z0-9_\\u4e00-\\u9fa5]+\\*[a-zA-Z0-9_\\u4e00-\\u9fa5]+)[-]?([a-zA-Z0-9_\\u4e00-\\u9fa5]*[\\*]?[a-zA-Z0-9_\\u4e00-\\u9fa5]*)").r
     regex_medusa_list_category_mv findFirstMatchIn  path match {
       case Some(p) =>  {
         println("matched ...")
         println(p.group(1))
         println(p.group(2))
       }
       case None => null
     }*/


    //println(("""[a-z]""".r findFirstMatchIn "A zimple example.") map (_.start))
    //println(("""[a-z]""".r findFirstMatchIn "A zimple example."))
    val list = List("home-search-CNWD", "home-kids_home-kids_seecartoon-search-SHIW")
    for (name <- list) {
      val result = matchRegex2(name)
      println(result)
    }
  }

}
