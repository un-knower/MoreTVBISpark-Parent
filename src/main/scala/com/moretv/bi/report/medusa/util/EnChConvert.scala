package com.moretv.bi.report.medusa.util

/**
 * Created by xiajun on 2016/5/17.
 *
 */
object EnChConvert {



  // 定义moretv中各个page中所对应的tab的中文信息
  def transformEng2Chinese(page:String,info:String)={
    var result:String = null
    if(page!=null){
      page match {
        case "hot" =>{
          if(info!=null){
            info match {
              case "search" => result = "搜索"
              case "hot_jiaodian" => result = "今日焦点"
              case "1_hot_tag_xinwenredian" => result = "新闻热点"
              case "hot_zhuanti" => result="资讯专题"
              case "1_hot_tag_chuangyidongzuo" => result="创意运动"
              case "1_hot_tag_yinshiduanpian" => result="影视短片"
              case "1_hot_tag_youxi" => result="游戏动画"
              case "danmuzhuanqu" => result="弹幕专区"
              case "1_hot_tag_qingsonggaoxiao" => result="轻松搞笑"
              case "1_hot_tag_shenghuoshishang" => result="生活时尚"
              case "1_hot_tag_yulebagua" => result="娱乐八卦"
              case "1_hot_tag_vicezhuanqu" => result="VICE专区"
              case "1_hot_tag_yinyuewudao" => result="音乐舞蹈"
              case "1_hot_tag_wuhuabamen" => result="五花八门"
              case "hot_lanmu" => result="短视频栏目"
              case "hot_dspzhuanti" => result="短视频专题"
              case _ => result=info
            }
          }
        }
        case "history" => {
          if(info!=null){
            info match {
              case "history" => result="观看历史"
              case "collect" => result="收藏追剧"
              case "subjectcollect" => result="专题收藏"
              case "mytag" | "tag" => result="标签订阅"
              case "reservation" =>result="节目预约"
              case _ => result=info
            }
          }
        }
        case "movie" => {
          if(info!=null){
            info match {
              case "search" => result="搜索"
              case "multi_search" => result="筛选"
              case "movie_hot" => result="院线大片"
              case "movie_7days" => result="七日更新"
              case "movie" => result="猜你喜欢"
              case "movie_jujiaodian" => result="电影聚焦点"
              case "movie_zhuanti" => result="电影专题"
              case "movie_teseyingyuan" => result="特色影院"
              case "movie_star" => result="影人专区"
              case "movie_yugao" => result="抢鲜预告"
              case "movie_yiyuan" => result="亿元票房"
              case "movie_xilie" => result="系列电影"
              case "movie_erzhan" => result="战争风云"
              case "movie_aosika" => result="奥斯卡佳片"
              case "movie_comic" => result="动画电影"
              case "movie_hollywood" => result="好莱坞巨制"
              case "movie_huayu" => result="华语精选"
              case "movie_yazhou" => result="日韩亚太"
              case "movie_lengmen" => result="冷门佳片"
              case "1_movie_tag_dongzuo" => result="犀利动作"
              case "1_movie_tag_kehuan" => result="科学幻想"
              case "movie_yueyu" => result="粤语佳片"
              case "collect" => result="已收藏"
              case "comic_erciyuan" => result="二次元"
              case "movie_night" => result="午夜场"
              case _ => result=info
            }
          }
        }
        case "tv" => {
          if(info!=null){
            info match {
              case "search" => result="搜索"
              case "multi_search" => result="筛选"
              case "tv_genbo" => result="华语热播"
              case "tv_zhuanti" => result="电视剧专题"
              case "dianshiju_tuijain" =>result="卫视常青剧"
              case "tv_kangzhanfengyun" => result="抗战风云"
              case "tv_meizhouyixing" => result="剧星专区"
              case "1_tv_area_xianggang" =>result="香港TVB"
              case "1_tv_area_hanguo" => result="韩剧热流"
              case "tv_julebu" => result="10亿俱乐部"
              case "tv_xianxiaxuanhuan" => result="仙侠玄幻"
              case "1_tv_area_neidi" => result="大陆剧场"
              case "1_tv_area_oumei" => result="特色美剧"
              case "tv_changju" => result="长剧欣赏"
              case "1_tv_area_riben" => result="日剧集锦"
              case "1_tv_area_taiwan" => result="台湾剧集"
              case "1_tv_area_yingguo" => result="英伦佳剧"
              case "1_tv_area_qita" => result="其他地区"
              case "tv_yueyu" => result="粤语专区"
              case "tv" => result="猜你喜欢"
              case "tv_jingdianchongwen"=>result="经典重温"
              case "tv_shujia" => result="最强暑假档"
              case "tv_yuansheng" => result = "电视原声"
              case "tv_guowai_lianzai" => result = "海外精选"
              case "tv_zhongbang" => result = "编辑精选"
              case "tv_jiatinglunli" => result = "家庭伦理"
              case "tv_dushiqinggan" => result = "都市情感"
              case "tv_ztj" => result = "择天记"
              case "tv_hls2" => result = "欢乐颂2"
              case "tv_summer" => result = "暑期最强档"
              case "tv_wdqbs" => result = "我的前半生"
              case "tv_tzb" => result = "狼性特种兵"
              case _ => result=info
            }
          }
        }
        case "zongyi" => {
          if(info != null) {
            info match {
              case "search" => result="搜索"
              case "multi_search" => result="筛选"
              case "p_zongyi_hot_1" => result="综艺热播"
              case "zongyi_weishi" => result="卫视强档"
              case "zongyi_zhuanti" => result="综艺专题"
              case "dalu_jingxuan" => result="大陆精选"
              case "hanguo_jingxuan" => result="韩国精选"
              case "oumei_jingxuan" => result="欧美精选"
              case "gangtai_jingxuan" => result="港台精选"
              case "zongyi_shaoer" => result="少儿综艺"
              case "1_zongyi_tag_zhenrenxiu" => result="真人秀场"
              case "1_zongyi_tag_fangtan" => result="情感访谈"
              case "1_zongyi_tag_youxi" => result="游戏竞技"
              case "1_zongyi_tag_gaoxiao" => result="爆笑搞怪"
              case "1_zongyi_tag_gewu" => result="歌舞晚会"
              case "1_zongyi_tag_shenghuo" => result="时尚生活"
              case "1_zongyi_tag_quyi" => result="说唱艺术"
              case "1_zongyi_tat_caijing" => result="财经民生"
              case "1_zongyi_tag_fazhi" => result="社会法制"
              case "1_zongyi_tag_bobao" => result="新闻播报"
              case "1_zongyi_tag_qita" => result="其他分类"
              case "haoshengying_zongyi" => result="2016新歌声"
              case "wangzhanzizhi_zongyi" => result="最强笑点"
              case "yulebagua_zongyi" => result = "娱乐八卦"
              case "xiangsheng_zongyi" => result = "相声小品"
              case "zongyi_wangpai" => result = "王牌综艺"
              case "zongyi_benpaoba" => result = "奔跑吧"
              case "zongyi_youxi" => result = "游戏竞技"
              case "jitiao_zongyi" => result = "极限挑战"
              case _ => result=info
            }
          }
        }
        case "kids_home" => {
          /*
          if(info!=null){
            info match {
              case "kids_recommend" => result="快乐的宝宝"
              case "kids_cathouse" => result="猫猫屋"
              case "kids_seecartoon" => result="看动画"
              case "kids_songhome" => result="听儿歌"
              case "kids_channel" => result="看频道"
            }
          }
          */
          result = info
        }
        case "comic" => {
          if(info != null) {
            info match {
              case "search" => result="搜索"
              case "multi_search" => result="筛选"
              case "hot_comic_top" => result="热播动漫"
              case "comic_zhujue" => result="动漫主角"
              case "dongman_xinfan" => result="新番上档"
              case "movie_comic" => result="动漫电影"
              case "comic_zhuanti" => result="动漫专题"
              case "comic_jingdian" => result="8090经典"
              case "comic_guoyu" => result="国语动画"
              case "comic_dashi" => result="动漫大师"
              case "comic_tags_jizhang" => result="机甲战斗"
              case "1_comic_tags_rexue" => result="热血冒险"
              case "1_comic_tags_gaoxiao" => result="轻松搞笑"
              case "1_comic_tags_meishaonu" => result="后宫萝莉"
              case "1_comic_tags_qingchun" => result="青春浪漫"
              case "1_comic_tags_lizhi" => result="励志治愈"
              case "1_comic_tags_huanxiang" => result="奇幻魔法"
              case "1_comic_tags_xuanyi" => result="悬疑推理"
              case "1_comic_tag_qita" => result="其他分类"
              case "comic_guoman"=>result="国漫精选"
              case "comic_mingzuo"=>result="名作之壁"
              case "comic_ciyuanjd" => result = "次元经典"
              case _ => result=info
            }
          }
        }
        case "mv" => {
          /*
          if(info != null) {
            info match {
              case "search" => result="搜索"
              case "multi_search" => result="筛选"
              case "collect" => result="我的收藏"
              case "station" => result="电台"
              case "1_mv_arrange_singer" => result="热门歌手"
              case "1_mv_arrange_liuxing" => result="正在流行"
              case "1_mv_arrange_shoufa" => result="MV首发"
              case "1_mv_arrange_subject" => result="精选集"
              case "1_mv_tags_yanchanghui" => result="演唱会"
              case "mv_top" => result="排行榜"
            }
          }
          */
          // 暂时不处理mv
          result = info
        }
        case "jilu" => {
          if(info!=null){
            info match {
              case "search" => result="搜索"
              case "multi_search" => result="筛选"
              case "p_document_1" => result="纪实热播"
              case "jishi_wangpai" => result="王牌栏目"
              case "jishi_zhuanti" => result="纪实专题"
              case "jilu_vice" => result="VICE专区"
              case "1_jilu_station_bbc" => result="BBC"
              case "1_jilu_station_ngc" => result="国家地理"
              case "1_jilu_station_nhk" => result="NHK"
              case "jilu_meishi" => result="美食大赏"
              case "1_jilu_tags_junshi" => result="军事风云"
              case "1_jilu_tags_ziran" => result="自然万象"
              case "1_jilu_tags_shehui" => result="社会百态"
              case "1_jilu_tags_renwu" => result="人物大观"
              case "1_jilu_tags_lishi" => result="历史构沉"
              case "1_jilu_tags_ted" => result="公开课"
              case "1_jilu_tags_keji" => result="前沿科技"
              case "1_jilu_tags_qita" => result="其他分类"
              case "jilu_maoshu"=>result="猫叔推荐"
              case "jilu_douban"=>result="豆瓣高分"
              case "jilu_xingzhen"=> result="刑侦档案"
              case "jilu_junshi" => result = "铁血军魂"
              case "jilu_qiche" => result = "大话汽车"
              case "jilu_lishi" => result = "历史奇闻"
              case "jilu_ziran" => result = "自然万象"
              case "jilu_teji" => result = "探险特辑"
              case _ => result=info
            }
          }
        }
        case "xiqu" => {
          if(info != null) {
            info match {
              case "search" => result="搜索"
              case "collect" => result="收藏"
              case "1_xiqu_tag_guangchangwu" => result="广场舞"
              case "1_zongyi_tag_quyi" => result="戏曲综艺"
              case "1_tv_xiqu_tag_jingju" => result="京剧"
              case "1_tv_xiqu_tag_yuju" => result="豫剧"
              case "1_tv_xiqu_tag_yueju" => result="越剧"
              case "1_tv_xiqu_tag_huangmeixi" => result="黄梅戏"
              case "1_tv_xiqu_tag_errenzhuan" => result="二人转"
              case "1_tv_xiqu_tag_hebeibangzi" => result="河北梆子"
              case "1_tv_xiqu_tag_jinju" => result="晋剧"
              case "1_tv_xiqu_tag_xiju" => result="锡剧"
              case "1_tv_xiqu_tag_qingqiang" => result="秦腔"
              case "1_tv_xiqu_tag_chaoju" => result="潮剧"
              case "1_tv_xiqu_tag_pingju" => result="评剧"
              case "1_tv_xiqu_tag_huaguxi" => result="花鼓戏"
              case "1_xiqu_tags_aoju" => result="粤剧"
              case "1_xiqu_tag_gezaixi" => result="歌仔戏"
              case "1_xiqu_tags_lvju" => result="吕剧"
              case "1_tv_xiqu_tag_huju" => result="沪剧"
              case "1_tv_xiqu_tag_huaiju" => result="淮剧"
              case "1_tv_xiqu_tag_chuanju" => result="川剧"
              case "1_tv_xiqu_tag_wuju" => result="婺剧"
              case "1_tv_xiqu_tag_kunqu" => result="昆曲"
              case "1_tv_xiqu_tag_suzhoutanchang" => result="苏州弹唱"
              case _ => result=info
            }
          }
        }
        case "subject" => {
          if(info!=null){
            info match {
              case "movie_zhuanti" => result="电影专题"
              case "tv_zhuanti" => result="电视剧专题"
              case "zongyi_zhuanti" =>result="综艺专题"
              case "comic_zhuanti" =>result="动漫专题"
              case "kid_zhuanti" =>result="少儿专题"
              case "hot_zhuanti" =>result="资讯专题"
              case "jilu_zhuanti" =>result="纪实专题"
              case "movie_star" =>result="影人专区"
              case "movie_xilie" =>result="系列电影"
              case "tv_meizhouyixing" =>result="剧星专题"
              case "zongyi_weishi" =>result="卫视强档"
              case "comic_dashi"=>result="动画大师"
              case _ => result=info
            }
          }
        }
        case _ => {
          result = info
        }
      }
    }
    result
  }

}
