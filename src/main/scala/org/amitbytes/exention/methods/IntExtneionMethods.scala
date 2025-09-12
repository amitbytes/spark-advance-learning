package org.amitbytes.exention.methods

object AllExtensionMethdods{
  implicit class StringExtneionMethods(val s: String) extends AnyVal {
    def ToInteger(): Int = {
      try{
        s.toInt
      } catch {
        case _: Exception => 0
      }
    }
    def ToDouble(): Double = {
      try {
        s.toDouble
      } catch {
        case _: Exception => 0.0
      }
    }
  }
}
