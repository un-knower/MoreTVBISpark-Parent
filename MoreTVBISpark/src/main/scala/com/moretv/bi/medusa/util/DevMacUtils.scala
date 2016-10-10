package com.moretv.bi.medusa.util

/**
  * Created by Will on 2016/3/27.
  */
object DevMacUtils {

  val macs= List("1048B14E92BC",
    "1882190C8112",
    "1882190C895E",
    "1048B151D936",
    "0066CF0B8EC2",
    "6cfaa74b4d26",
    "E8BB3D32DDB3",
    "1882190C78F6",
    "D896E049AD3A",
    "38FACA0A024F",
    "9CA69D100017",
    "0066CF0D1188",
    "d85de249c61c",
    "0066DE05270A",
    "0066CF00BDB5",
    "1048b1c4bf20",
    "12212B35782C",
    "FAE046D4DB67",
    "009ec8531637",
    "4419b6aa3d63",
    "5CC6D0C22777",
    "C80E774CA5CE",
    "1048B11A1D2B",
    "D896E0C95E6C",
    "0004A3123994",
    "D896E0004FA2",
    "1C8E5CE1A76A",
    "D896E0AF7296",
    "b0a37e2f99c1",
    "68f06d0346f6",
    "00226D0C5011",
    "001A34BFB71E",
    "00ce39bb06e5",
    "208b3725e39a",
    "606DC79FA77C",
    "1048B151DEE7")

  val userIds = List("8a905c67a4d5eee0d87bdbeba14ff843",
    "5aed28133ea0d6e6d487f453741e5b7c",
    "d9d2bd0433104541820ff36928c38c11",
    "bbcb6f0749e0ee8be17bd18f84771b91",
    "78ed1ab6ef3438861cb7e6a94adedf00",
    "6e3b2697585f32f047d66bd6d47c4cf2",
    "b1a46d566d937c26d56d0ff60c46b014",
    "5d362b3aeaa8a113891d773781665c89",
    "42cb538b2271cd5625766831f46fb6c2",
    "ad1947b851e1dcda0571cde0cb5061d2",
    "4571c8987cbf06f1b7c507eeebf88c2c",
    "546a16e9faa04985b17327db41e00284",
    "38d17cbca3f600c6051554a6dccbf802",
    "2a24d76468692def3cbfab82e429aa0b",
    "a6e0b4635e469e241a1e70c8d8da8457",
    "6a19758b7bf888d6a4fc6105c0382bcd",
    "cee6b007921fe166157c97b1ebed55b6",
    "6f32a014bc5d2f7191f3ee8a3a01bcda",
    "038ca95c29564f47ed70d90046029651",
    "ea771145eaaa5485d580492ef524aaa9",
    "6a076e199084ae4410a7636830f8901c",
    "1a732e3e72eaa5344c60486d03cd8017",
    "360f3eced9875205f58096d9d9caae19",
    "41f28978f9f21d1ed242d476dd8b6470",
    "199bc62f091f6aceaba062f935115843",
    "58e6a71b9cea07bb0d5324e1ad15e600",
    "8dbf76ad7c9983f446f7b62551847943",
    "4dfc80753a4ecceaba04c7a4b59e597e",
    "0903bb3167cda9152036c99654a337bd",
    "e3c6a267f50dd140fc8024d8648d9808",
    "d5f0384e7295f1582fee9401f5f603a2",
    "742ddb93824564eb2bdccf8723b1513d",
    "a96884da344b86bd0187b30e6850acc8")

  def macFilter(mac:String) = macs.exists(_.equalsIgnoreCase(mac))

  def userIdFilter(userId:String) = userIds.exists(_.equalsIgnoreCase(userId))

}
