package cn.ac.iie.dataImpl.dpi

object DpiSample {

  def main(args: Array[String]): Unit = {
    val str = "377|0731|2A|1745990892184817010|103|1505643890552096|1505643890701360|||01|06|0021|000|0|0|117.136.89.178||59389|0|183.232.74.122||80|1035|529|5|3|149264|85808|0|0|0|0|32906|37107|0|0|9358|64863|87808|1360|1|0|1|1|1|0|0|0|1572869|0|720897|0|||||||||||||0x03|5|200|64863|64863|64863|cloudlist.service.kugou.com|45|cloudlist.service.kugou.com/v1/get_singerlist||66|Android601-AndroidPhone-8851-14-0-MyFocusSingerListRequestor-cmnet|text/plain|||||0||3|0|149264|0|0|||1||0|||||||||||"
    println(str.split("\\|",-1).length)
  }

}
