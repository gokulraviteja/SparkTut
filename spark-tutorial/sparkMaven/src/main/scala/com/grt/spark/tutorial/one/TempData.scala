package com.grt.spark.tutorial.one



case class TempData(day: Int, doy: Int, month: Int, year: Int,
                    precip: Double, snow: Double, tave: Double, tmax: Double, tmin: Double)

object TempData {
  def toDoubleOrNeg(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case _: NumberFormatException => -1
    }
  }
}


