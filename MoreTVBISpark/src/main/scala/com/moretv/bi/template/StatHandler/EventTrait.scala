package com.moretv.bi.template.StatHandler

/**
  * Created by witnes on 11/3/16.
  */
trait EventTrait {


  val PLAY_TIMES_EVENTS = Array("'startplay'", "'playview'")

  val PLAY_TIMES_2_EVENTS = Array("playview")

  val PLAY_TIMES_3_EVENTS = Array("startplay")

  val PLAY_DURATION_EVENTS = Array("'userexit'", "'selfend'")

  val LIVE_TIMES_EVENTS = Array("'startplay'")

  val LIVE_DURATION_EVENTS = Array("'switchchannel'")

}
