package com.moretv.bi.report.medusa.util

/**
 * Created by Administrator on 2016/4/14.
 */
object MedusaLogInfoUtil {

  /*logTypeӳ��*/
  val LAUNCHVIEW = "homeview"
  val LAUNCHCLICK = "homeaccess"
  val LIVE = "live"
  val PLAY = "play"
  val ENTER = "enter"
  val EXIT = "exit"
  val DETAIL = "detail"
  val INTERVIEW = "interview"

  /*pathMain�еĵڶ����ֶε�ӳ�䣬����*�ָ�����*/
  val RECOMMENDATION = "recommendation"
  val MYTV = "my_tv"
  val CLASSIFICATION = "classification"
  val SEARCH = "home-search"


  /*accessArea��ʶ*/
  val RECOMMENDATION_AC = "recommendation"
  val HOT_SUBJECT_AC = "hotSubject" //短视频
  val TASTE_AC = "taste" //兴趣推荐

  val SEARCHSET_AC = "navi"
  val SEARCH_AL = "search"  //搜索
  val SET_AL = "setting"    //设置

  val MYTV_AC = "my_tv"
  val HISTORY_MYTV_AL = "history" //历史
  val COLLECT_MYTV_AL = "collect" //收藏
  val ACCOUNT_MYTV_AL = "account" //我
  val SITEMOVIE_MYTV_AL = "site_movie" //电影
  val SITETV_MYTV_AL = "site_tv" //电视剧
  val SITEZONGYI_MYTV_AL = "site_zongyi" //综艺
  val SITESPORT_MYTV_AL = "site_sport" //体育
  val SITECOMIC_MYTV_AL = "site_comic"//动漫
  val SITEJILU_MYTV_AL = "site_jilu"//纪实
  val SITEHOT_MYTV_AL = "site_hot"//资讯短片
  val SITEMV_MYTV_AL = "site_mv"//音乐
  val SITEKIDS_MYTV_AL = "site_kids"//少儿
  val SITEXIQU_MYTV_AL = "site_xiqu"//戏曲
  val SITEAPPLICATION_MYTV_AL = "site_application"//应用推荐
  val SITELIVE_MYTV_AL = "site_live"//直播

  val FOUNDATION_AC = "foundation"
  val INTERESTLOCATION_FOUNDATION_AL = "interest_location" //兴趣坐标
  val TOPNEW_FOUNDATION_AL = "top_new" //新剧榜
  val TOPHOT_FOUNDATION_AL = "top_hot" //热播榜
  val TOPSTAR_FOUNDATION_AL = "top_star" //明星榜
  val TOPCOLLECT_FOUNDATION_AL = "top_collect" //收藏榜


  val CLASSIFICATION_AC = "classification"
  val MOVIE_CLASSIFICATION_AL = "movie" //电影
  val TV_CLASSIFICATION_AL = "tv" //电视剧
  val ZONGYI_CLASSIFICATION_AL = "zongyi" // 综艺
  val SPORT_CLASSIFICATION_AL = "sport" //体育
  val COMIC_CLASSIFICATION_AL = "comic"//动漫
  val JILU_CLASSIFICATION_AL = "jilu"//纪实
  val HOT_CLASSIFICATION_AL = "hot"//资讯短片
  val MV_CLASSIFICATION_AL = "mv"//音乐
  val KIDS_CLASSIFICATION_AL = "kids"//少儿
  val XIQU_CLASSIFICATION_AL = "xiqu"//戏曲
  val APPLICATION_CLASSIFICATION_AL = "application"//应用推荐

  val LIVE_AC = "live"
  val ONE_LIVE_LA = "1"
  val TWO_LIVA_LA ="2"
  val THREE_LIVE_LA = "3"
  val FOUR_LIVE_LA = "4"
  val FIVE_LIVE_LA ="5"
  val SIX_LIVE_LA = "6"
  val SEVEN_LIVE_LA = "7"
  val EIGHT_LIVE_LA = "8"
  val NINE_LIVE_LA = "9"
  val TEN_LIVE_LA = "10"
  val ELEVEN_LIVE_LA = "11"
  val TWELEVE_LIVE_LA = "12"
  val THIRTEEN_LIVE_LA = "13"



  /*medusa�и���logType�и����ֶ�����ֵ֮���ӳ��*/
  //homeview
  val ACCOUNTID_HOMEVIEW = 0
  val APKSERIES_HOMEVIEW = 1
  val APKVERSION_HOMEVIEW =2
  val DATE_HOMEVIEW = 3
  val DATETIME_HOMEVIEW = 4
  val DURATION_HOMEVIEW = 5
  val EVENT_HOMEVIEW = 6
  val PRODUCTMODE_HOMEVIEW = 12
  val PROMOTION_HOMEVIEW = 13
  val USERID_HOMEVIEW = 15
  //homeaccess
  val ACCESSAREA_HOMEACCESS = 0
  val ACCESSLOCATION_HOMEACCESS = 1
  val ACCOUNTID_HOMEACCESS = 2
  val APKSERIES_HOMEACCESS= 3
  val APKVERSION_HOMEACCESS =4
  val DATE_HOMEACCESS = 5
  val DATETIME_HOMEACCESS = 6
  val EVENT_HOMEACCESS = 7
  val LOCATIONINDEX_HOMEACCESS = 11
  val PRODUCTMODE_HOMEACCESS = 14
  val PROMOTION_HOMEACCESS = 15
  val USERID_HOMEACCESS = 16
  //play
  val ACCOUNTID_PLAY = 0
  val APKSERIES_PLAY = 1
  val APKVERSION_PLAY =2
  val CONTENTTYPE_PLAY = 3
  val DATE_PLAY = 4
  val DATETIME_PLAY = 5
  val DURATION_PLAY = 6
  val EPISODESID_PLAY = 7
  val EVENT_PLAY = 8
  val PATHMAIN_PLAY = 14
  val PATHSPECIAL_PLAY =15
  val PATHSUB_PALY =16
  val PRODUCTMODE_PLAY = 17
  val PROMOTION_PLAY = 18
  val RETRIEVAL_PLAY = 19
  val SEARCHTEXT_PLAY = 20
  val USERID_PALY = 22
  val VIDEOSID_PLAY = 23






  def identifyNameMapping(flag:String)={
    flag match {
      case "hotSubject" => "短视频"
      case "taste" => "兴趣推荐"
      case "navi"=>"搜索与设置"
      case "my_tv"=>"我的电视"
      case "recommendation"=>"今日推荐"
      case "foundation"=>"发现"
      case "classification"=>"分类"
      case "site_live" | "live"=>"直播"
      case "search"=>"搜索"
      case "setting"=>"设置"
      case "history"=>"历史"
      case "collect"=>"收藏"
      case "account"=>"我"
      case "site_movie" | "movie"=>"电影"
      case "site_tv" | "tv"=>"电视剧"
      case "site_zongyi" | "zongyi"=>"综艺"
      case "site_sport" | "sport" => "体育"
      case "site_comic" | "comic"=>"动漫"
      case "site_jilu" | "jilu"=>"纪实"
      case "site_hot" | "hot"=>"资讯短片"
      case "site_mv" | "mv"=>"音乐"
      case "site_kids" | "kids"=>"少儿"
      case "site_xiqu" | "xiqu"=>"戏曲"
      case "site_application" | "application" => "应用推荐"
      case "interest_location"=>"兴趣坐标"
      case "top_new"=>"新剧榜"
      case "top_hot"=>"热播榜"
      case "top_star"=>"明星榜"
      case "top_collect"=>"收藏榜"
      case "1"=>"区域1"
      case "2"=>"区域2"
      case "3"=>"区域3"
      case "4"=>"区域4"
      case "5"=>"区域5"
      case "6"=>"区域6"
      case "7"=>"区域7"
      case "8"=>"区域8"
      case "9"=>"区域9"
      case "10"=>"区域10"
      case "11"=>"区域11"
      case "12"=>"区域12"
      case "13"=>"区域13"
      case "14"=>"区域14"
      case "LetvNewC1S"=>"LetvNewC1S"
      case "we20s"=>"we20s"
      case "M321"=>"M321"
      case "MagicBox_M13"=>"MagicBox_M13"
      case "MiBOX3"=>"MiBOX3"



    }
  }
}
