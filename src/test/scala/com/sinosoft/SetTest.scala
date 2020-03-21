package com.sinosoft

import scala.collection.mutable

object SetTest {
  def main(args: Array[String]): Unit = {
    val s = "ئىرادىنى ساقلاپ، ئىشەنچنى كۈچەيتىپ، زېھنى كۈچنى مەركەزلەشتۈرۈپ ئۆز ئىشىمىزنى ياخشى قىلىش تۈرلۈك خەۋپ – خەتەر، خىرىسلارغا تاقابىل تۇرۇشىمىزدىكى ئاچقۇچ."
    val arr =s.toCharArray
    arr.foreach(println(_))

  }
}
