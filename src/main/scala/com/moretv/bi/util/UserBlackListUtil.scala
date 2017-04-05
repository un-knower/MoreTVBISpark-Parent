package com.moretv.bi.util

/**
  * Created by Will on 2016/8/3.
  * 用户黑名单，存放总是产生脏数据的用户的userId
  */
@deprecated
object UserBlackListUtil {

  private val blackList = List("eee3fe94a7b248105ae8507e6ac641af",
                              "424aed3ec5469e7bfd0fc85454755fd5",
                              "3cae337e9d224cec371603f38fe4a805",
                              "e674cafde0a88c41046d00aad67b5418",
                              "e2909057d6b58cba4468465ef5147f52",
                              "cf5e9a43a77bec73068ab76e82334455",
                              "e83409e0a1747a212fe332c3cd121223",
                              "64dd1b03a7e22c8158e6020238b16250",
                              "0cc601b892ee2e945c3ef9281b20205a",
                              "5585e2661a602ad309e5ed1575c41ecc",
                              "964444be3a566c37ffde2794829c28d0",
                              "3894ef30711fb6688612769d0f9752df",
                              "95aa26a203b892447f54c9cc4bddfa02",
                              "03d947674a5027d4a13480efddecbcdd",
                              "ce306024aff1c2ec1f5c7fa53eb8d279",
                              "e5f09f8b89bd3488bb72963e226c0745")

  /**
    * 判断日志中是否包含黑名单用户的userId
    * @param log 原始日志
    * @return 是否为黑名单用户
    */
  def isBlack(log:String) = {
    blackList.exists(log.contains)
  }

  def isNotBlack(log:String) = !isBlack(log)

}
