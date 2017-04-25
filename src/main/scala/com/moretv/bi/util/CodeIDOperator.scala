package com.moretv.bi.util

import java.util
/**
 * Created by Administrator on 2016/12/8.
 */
object CodeIDOperator {
  val stringkey: String = "a1b2cd3e4f5ghi6jklm7nop8qrstu9vwx0yz"
  val numberKey: Array[Integer] = Array(1, 86, 21, 62, 59, 17)

  def idToCode(id: Int): String = {
    val mfId: String = String.valueOf(id)
    if (mfId.length <= 10) {
      val list = getKey
      val numString: List[String] = list.get(0).asInstanceOf[List[String]]
      var i: Int = 1
      import scala.collection.JavaConversions._
      for (c <- numString) {
        i += 1
      }
      val idl: Int = mfId.length - 1
      val len: Int = 11 - idl
      var stringId: String = idl + "" + mfId + ""
      (0 until len).foreach(i=>{
        stringId += (Math.random * 10).toInt
      })

      var code: String = ""
      (0 until 6).foreach(j=>{
        val a: String = String.valueOf(stringId.charAt(2 * j))
        val b: String = String.valueOf(stringId.charAt(2 * j + 1))
        val num: Int = (Integer.valueOf(a + b) + numberKey(j)) % 100
        code += numString.get(num)
      })
      code
    }else null
  }

  def codeToId(code: String): Int = {
    var id: Integer = 0
    if (code.length == 12) {
      val list = getKey
      val stringNum: util.HashMap[String, Int] = list.get(1).asInstanceOf[util.HashMap[String, Int]]
      var stringId: String = ""
      (0 until 6).foreach(j=>{
          val a: String = String.valueOf(code.charAt(2 * j))
          val b: String = String.valueOf(code.charAt(2 * j + 1))
          var n: Integer = stringNum.get(a + b)
          n = n - numberKey(j)
          if (n < 0) {
            n = n + 100
          }
          var ns: String = String.valueOf(n)
          if (ns.length < 2) {
            ns = "0" + ns
          }
          stringId += ns
      })
    id = Integer.valueOf(stringId.substring(1, String.valueOf(stringId.charAt(0)).toInt + 2))
    }
    id
  }

  def getKey = {
    val list = new util.ArrayList[Object]()
    val numString = new util.ArrayList[String]()
    val stringNum:util.HashMap[String,Int] = new util.HashMap[String, Int]()
    (0 until 100).foreach(i=>{
      val strTmp: String = String.valueOf(stringkey.charAt(i % 33)) + String.valueOf(stringkey.charAt((i % 33) + 1 + (i / 33)))
      numString.add(strTmp)
      stringNum.put(strTmp,i)
    })
    list.add(numString)
    list.add(stringNum)
    list
  }
}
