package com.moretv.bi.report.medusa.util.udf

/**
 * Created by Admin on 2016/5/11.
 * 定义UDF中所需要的常量
 */
object UDFConstant {

  /**
   * For PathParser
   * *******************************************************************
   */
  // 定义日志类型：logType
  val INTERVIEW = "interview"
  val DETAIL = "detail"
  val PLAYVIEW = "playview" //(注：对于medusa而言，为play日志；对于moretv而言，为playview日志)
  val PLAY = "play"         // For medusa

  // 定义路径字段类型：pathType
  val PATH = "path"                    // For moretv
  val PATHMAIN = "pathMain"            // For medusa
  val PATHSUB = "pathSub"              // For medusa
  val PATHSPECIAL = "pathSpecial"      // For medusa

  // 定义输出类型：outpType
  val CONTENTTYPE = "contentType"      // For moretv

  // pathSpecial
  val PATHPROPERTY = "pathProperty"    // For medusa,从pathSpecial中获取
  val PATHIDENTIFICATION = "pathIdentification" // For medusa,从pathSpecial中获取
  // pathSub
  val ACCESSPATH = "accessPath"        // For medusa,从pathSub中获取
  val PREVIOUSSID = "previousSid"      // For medusa,从pathSub中获取
  val PREVIOUSCONTENTTYPE = "previousContentType"   // For medusa,从pathSub中获取
  // pathMain
  val LAUNCHERAREA = "launcherArea"    // For medusa,从pathMain中获取
  val LAUNCHERACCESSLOCATION = "launcherAccessLocation"    // For medusa,从pathMain中获取
  val PAGETYPE = "pageType"            // For medusa,从pathMain中获取
  val PAGEDETAILINFO = "pageDetailInfo"  // For medusa,从pathMain中获取

  // path
  // 定义moretv的launcherArea集合
  val MoretvLauncherAreaNAVI = Array("search","setting") //For moretv,navi包含了search和setting两种
    //For moretv, 将这些内容归属于分类信息中
  val MoretvLauncherCLASSIFICATION = Array("history","movie","tv","live","hot","zongyi","comic","mv","jilu","xiqu","sports",
      "kids_home","subject")
  val MoretvLauncherUPPART = Array("watchhistory","otherswatch","hotrecommend","TVlive")
  // 定义moretv的launcher的accessArea集合
  val MoretvLauncherAccessLocation = Array("search","setting","history","movie","tv","live","hot","zongyi","comic","mv",
    "jilu","xiqu","sports","kids_home","subject")
  val MoretvPageInfo = Array("history","movie","tv","zongyi","hot","comic","mv","xiqu","sports","jilu","subject","live",
    "search","kids_home")
  val MoretvPageDetailInfo = Array("search","hot_jiaodian","1_hot_tag_xinwenredian","hot_zhuanti","jilu_teji",
    "1_hot_tag_chuangyidongzuo","1_hot_tag_yinshiduanpian","1_hot_tag_youxi","danmuzhuanqu","1_hot_tag_qingsonggaoxiao",
    "1_hot_tag_shenghuoshishang","1_hot_tag_yulebagua","1_hot_tag_vicezhuanqu","1_hot_tag_yinyuewudao","hot_lanmu","hot_dspzhuanti",
    "1_hot_tag_wuhuabamen","history","collect","subjectcollect","mytag","tag","reservation","multi_search","movie_hot",
    "movie_7days","movie","movie_jujiaodian","movie_zhuanti","movie_teseyingyuan","movie_star","movie_yugao","zongyi_youxi",
    "movie_yiyuan","movie_xilie","movie_erzhan","movie_aosika","movie_comic","movie_hollywood","movie_huayu","zongyi_benpaoba",
    "movie_yazhou","movie_lengmen","1_movie_tag_dongzuo","1_movie_tag_kehuan","movie_yueyu","collect","tv_genbo","tv_cqz","tv_summer",
    "tv_zhuanti","dianshiju_tuijain","tv_kangzhanfengyun","tv_meizhouyixing","1_tv_area_xianggang","1_tv_area_hanguo","tv_jiatinglunli","tv_dushiqinggan",
    "tv_julebu","tv_xianxiaxuanhuan","1_tv_area_neidi","1_tv_area_oumei","tv_changju","1_tv_area_riben","jilu_xingzhen","jilu_ziran",
    "1_tv_area_taiwan","1_tv_area_yingguo","1_tv_area_qita","tv_yueyu","tv","p_zongyi_hot_1","zongyi_weishi","jitiao_zongyi",
    "zongyi_zhuanti","dalu_jingxuan","hanguo_jingxuan","oumei_jingxuan","gangtai_jingxuan","zongyi_shaoer","xiangsheng_zongyi",
    "1_zongyi_tag_zhenrenxiu","1_zongyi_tag_fangtan","1_zongyi_tag_youxi","1_zongyi_tag_gaoxiao","1_zongyi_tag_gewu","zongyi_wangpai",
    "1_zongyi_tag_shenghuo","1_zongyi_tag_quyi","1_zongyi_tat_caijing","1_zongyi_tag_fazhi","1_zongyi_tag_bobao","yulebagua_zongyi",
    "1_zongyi_tag_qita","hot_comic_top","comic_zhujue","dongman_xinfan","movie_comic","comic_zhuanti","comic_jingdian",
    "comic_guoyu","comic_dashi","comic_tags_jizhang","1_comic_tags_rexue","1_comic_tags_gaoxiao","1_comic_tags_meishaonu",
    "1_comic_tags_qingchun","1_comic_tags_lizhi","1_comic_tags_huanxiang","1_comic_tags_xuanyi","1_comic_tag_qita","comic_erciyuan","comic_ciyuanjd",
    "p_document_1","jishi_wangpai","jishi_zhuanti","jilu_vice","1_jilu_station_bbc","1_jilu_station_ngc","jilu_junshi","jilu_qiche",
    "1_jilu_station_nhk","jilu_meishi","1_jilu_tags_junshi","1_jilu_tags_ziran","1_jilu_tags_shehui","1_jilu_tags_renwu","jilu_lishi",
    "1_jilu_tags_lishi","1_jilu_tags_ted","1_jilu_tags_keji","1_jilu_tags_qita","1_xiqu_tag_guangchangwu",
    "1_zongyi_tag_quyi","1_tv_xiqu_tag_jingju","1_tv_xiqu_tag_yuju","1_tv_xiqu_tag_yueju","1_tv_xiqu_tag_huangmeixi","tv_hls2","tv_ztj",
    "1_tv_xiqu_tag_errenzhuan","1_tv_xiqu_tag_hebeibangzi","1_tv_xiqu_tag_jinju","1_tv_xiqu_tag_xiju",
    "1_tv_xiqu_tag_qingqiang","1_tv_xiqu_tag_chaoju","1_tv_xiqu_tag_pingju","1_tv_xiqu_tag_huaguxi","1_xiqu_tags_aoju",
    "1_xiqu_tag_gezaixi","1_xiqu_tags_lvju","1_tv_xiqu_tag_huju","1_tv_xiqu_tag_huaiju","1_tv_xiqu_tag_chuanju",
    "1_tv_xiqu_tag_wuju","1_tv_xiqu_tag_kunqu","1_tv_xiqu_tag_suzhoutanchang","movie_zhuanti","tv_zhuanti","tv_zhongbang","tv_wdqbs",
    "zongyi_zhuanti","comic_zhuanti","kid_zhuanti","hot_zhuanti","jilu_zhuanti","movie_star","movie_xilie","movie_night",
    "tv_meizhouyixing","zongyi_weishi","comic_dashi","tv_jingdianchongwen","comic_guoman","jilu_maoshu","jilu_douban",
    "comic_mingzuo","tv_shujia","haoshengying_zongyi","wangzhanzizhi_zongyi","tv_yuansheng","tv_guowai_lianzai","tv_tzb",
    "kids_schoolstart","kids_televison","music_zongyi","tv_hkyy","comic_maoxian","comic_gaoxiao","tv_xiju","tv_malataiju",
    "tv_free","tv_paihang","tv_gaofen","tv_wangju","mengwa_zongyi","jixiantiaozhan_zongyi","jilu_vip","tv_yiliao","tv_vip","tv_guowai_lianzai")




  val MORETVCONTENTTYPE = Array("history","movie","tv","zongyi","hot","comic","mv","xiqu","sports","jilu","subject")
  val MORETVPAGETABINFOFOUR = Array("kids_home")
  val MORETVPAGETABINFOTHREE = Array("history","movie","tv","zongyi","hot","comic","mv","xiqu","jilu","subject")
  val MORETVPATHSUBCATEGORY = Array("similar","peoplealsolike","guessyoulike")
  val MORETVPATHSPECIALCATEGORY = Array("tag","subject","star")





  val MedusaPathSubAccessPath = Array("similar","peoplealsolike","guessyoulike")
  val MedusaPathProperty = Array("subject","tag","star")

  // 定义medusa的launcherArea集合、launcherAccessLocation集合
  val MedusaLauncherArea = Array("classification", "my_tv", "live", "recommendation", "foundation", "navi", "hotSubject", "taste")
  val MedusaLauncherAccessLocation = Array("history","collect","account","movie","tv","zongyi","jilu","comic","xiqu",
    "kids","hot","mv","sport","top_new","top_hot","top_star","top_collect","interest_location",
    "0","1","2","3","4","5","6","7","8","9","10","11","12","13","14")
  val MedusaLive = "live"          // 用于处理直播的特殊情况
  // 定义medusa的page页面的类型
  val MedusaPageInfo = Array("rank","everyone_watching","history","search","accountcenter_home","movie","tv","zongyi","jilu",
      "comic","xiqu","kids_home","hot","mv","sport_home")
  // 定义page页面中的详细信息
  val MedusaPageDetailInfo = Array(
    "搜索","筛选",
    "收藏追看","节目预约","标签订阅","专题收藏","明星关注",
    "热播动漫","动漫主角","新番上档", "动漫电影","动漫专题","8090经典","国语动画","动画大师","机甲战斗","热血冒险","轻松搞笑","后宫萝莉",
    "青春浪漫","励志治愈","奇幻魔法","悬疑推理","其他分类","国漫精选","名作之壁","二次元","日漫精选","游戏竞技","人物特辑",
    "everyone_nearby","top_new","top_hot","top_star","top_collect","欢乐颂2","择天记","自然万象","狼性特种兵",
    "院线大片","七日更新","猜你喜欢", "电影聚焦点","电影专题","特色影院","影人专区","抢先预告","亿元票房","系列电影","战争风云","奥斯卡佳片",
    "动画电影","好莱坞巨制","华语精选","日韩亚太","冷门佳片","犀利动作","科学幻想","粤语佳片","编辑精选","午夜场","楚乔传","极限挑战",
    "华语热播","电视剧专题","卫视常青剧", "战争风云","剧星专区","香港TVB","韩剧热流","10亿俱乐部","仙侠玄幻","大陆剧场","特色美剧","长剧欣赏",
    "日剧集锦","台湾剧集","英伦佳剧","其他地区","粤语专区","猜你喜欢","经典重温","金秋大剧档","电视原声","海外精选","奔跑吧","家庭伦理","都市情感",
    "综艺热播","卫视强档","综艺专题", "大陆精选","韩国精选","欧美精选","港台精选","少儿综艺","真人秀场","情感访谈","游戏竞技","爆笑搞怪","历史奇闻",
    "歌舞晚会","时尚生活","说唱艺术","财经民生","社会法制","新闻播报","其他分类","2016新歌声","最强笑点","娱乐八卦",
    "纪实热播","王牌栏目","纪实专题", "VICE专区","BBC","国家地理","NHK","美食大赏","军事风云","自然万象","社会百态","人物大观",
    "历史钩沉","公开课","前沿科技","其他分类","豆瓣高分","猫叔推荐","刑侦档案","相声小品","铁血军魂","大话汽车","王牌综艺",
    "我的收藏","今日焦点","新闻热点", "资讯专题","创意运动","影视短片","游戏动画","生活时尚","娱乐八卦","音乐舞蹈","五花八门","我的前半生",
    "电台","热门歌手","正在流行", "MV首发","精选集","演唱会","排行榜","短视频栏目","短视频专题",
    "广场舞","戏曲综艺","京剧", "豫剧","越剧","黄梅戏","二人转","河北梆子","晋剧","锡剧","秦腔","潮剧",
    "评剧","花鼓戏","粤剧","歌仔戏","吕剧","沪剧","淮剧","川剧","婺剧","昆曲","苏州弹唱",
    "kids_collect*观看历史","kids_collect*收藏追看","kids_collect*专题收藏", "kids_anim*动画明星","kids_anim*少儿热播",
    "kids_anim*动画专题","kids_anim*少儿电影","kids_anim*儿童综艺","kids_anim*0-3岁","kids_anim*最新出炉",
    "kids_anim*4-6岁","kids_anim*7-10岁","kids_anim*英文动画","kids_anim*搞笑","kids_anim*机战","kids_anim*亲子",
    "kids_anim*迪士尼专区","kids_anim*探险","kids_anim*中文动画","kids_anim*亲子交流","kids_anim*益智启蒙","kids_anim*童话故事",
    "kids_anim*教育课堂","kids_rhymes*随便听听",
    "kids_rhymes*儿歌明星","kids_rhymes*儿歌热播","kids_rhymes*儿歌专题","kids_rhymes*英文儿歌","kids_rhymes*舞蹈律动",
    "kids_anim*同步卫视","kids_anim*开学季","热门音乐","花开月圆","热血冒险","轻松搞笑","口碑韩剧","精选美剧","古装玄幻","特种兵营",
    "爆笑喜剧","麻辣泰剧", "百看不厌","热播排行","豆瓣高分","超级网剧", "极限挑战","萌娃宝贝","青春偶像","家庭情感","独家策划","VIP纪实","医者仁心",
  "电视剧会员","海外精选")



}
