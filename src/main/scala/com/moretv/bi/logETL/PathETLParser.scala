package com.moretv.bi.logETL

/**
  * Created by chubby on 2017/3/29.
  * This Function is used to parse the path info, and deal with some un-standard data in path based on the dim table in DW
  * Case 1: home*classification*movie-movie*院线大片
  * Case 2: kids_home-kandonghua*国漫精选
  */
object PathETLParser {


  def pathMainParse(path:String) = {
    var firstLevel:String = null
    var secondLevel:String = null
    var thirdLevel:String = null
    var fourthLevel:String = null
    var fifthLevel:String = null
    if(path != null && path != ""){

      /**Step1: Split the pathMain by '-', and get the path array info*/
      var pathArr:Array[String] = null
      if(path.contains("-")){
        pathArr = path.split("-")
      }else pathArr = Array(path)
      val pathArrLen = pathArr.length

      if(pathArrLen == 1){
        var (enterWay,area,location) = parseEnterWay(pathArr(0))
        var (pageInfo,tabInfo) = parseListCategory(pathArr(0))

        if(path.contains(Constants.KIDS_FLAG)){
          /**路径中显式确定少儿频道*/
          if(pageInfo == null && tabInfo ==null) {
            /*路径中不包含列表信息*/
            Regexs.KIDS_3X_RECOMMEND_REGEX findFirstMatchIn path match {
              case Some(p) => {
                pageInfo = "kids"
                tabInfo = "recommendation"
              }
              case None =>
            }


          }

        }else if(path.contains(Constants.SPORTS_FLAG)){
          /**路径中显式确定体育频道*/

        }else if(path.contains(Constants.MV_FLAG)) {
          /**路径中显式确定音乐频道*/

        }else{

        }

      }else if(pathArrLen >= 2) {
        val (enterWay,area,location) = parseEnterWay(pathArr(0))
        val (pageInfo,tabInfo) = parseListCategory(pathArr(pathArrLen-1))
      }


    }
  }


  /**
    * This function is used to parse the info of list category
    * @param str
    * @return pageInfo,tabInfo
    *         eg:tv,院线大片
    *            tingerge,随便听听
    */
  def parseListCategory(str:String) = {
    var res1:String = null
    var res2:String = null
    Regexs.LIST_CATEGORY_REGEX findFirstMatchIn str match {
      case Some(p) => {
        res1 = p.group(1)
        res2 = p.group(2)
      }
      case None =>
    }
    (res1,res2)
  }

  /**
    * This function is used to parse the application enter way, which is including two methods: in-application and third-application
    * if in-application then ''home''
    * if third-application then ''third''
    * @param str
    * @return
    */
  def parseEnterWay(str:String) = {
    var res1:String = null
    var res2:String = null
    var res3:String = null
    Regexs.ENTER_WAY_REGEX findFirstMatchIn str match {
      case Some(p) => {
        val pgLen = p.groupCount
        res1 = p.group(1)
        if (pgLen == 2) {
          val p2 = p.group(2)
          val (p2Arr, len) = splitStr(p2, "*")
          if (len == 2) {
            val tmp = p2Arr(1)
            if (Constants.LAUNCHERAREA.contains(tmp)) {
              res2 = tmp
            } else {
              /** Other situation */
            }
          } else if (len == 3) {
            val tmp = p2Arr(1)
            if (Constants.LAUNCHERAREA.contains(tmp)) {
              res2 = tmp
              res3 = p2Arr(2)
            } else {
              /** Other situation */
            }
          }
        }
      }
      case None => {
        res1 = Constants.THIRDPART_FLAG
      }
    }
    (res1,res2,res3)
  }



  /**
    * Split some string based on by
    * The $string should not be null, the $string should not be the same as $by and the string should contains $by
    * @param str
    * @param by
    * @return
    */
  def splitStr(str:String,by:String) = {

    /**转义*/
    var splitBy:String = null
    if(by.equals("*")) splitBy = "\\*" else splitBy = by

    if(str != null && !str.equals(by) && str.contains(by)) {
      val arr = str.split(splitBy)
      (arr,arr.length)
    }else{
      (Array[String](),0)
    }
  }

}
