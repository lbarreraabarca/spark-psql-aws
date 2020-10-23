package com.analytic.entities.utils

import scala.util.Try
import java.time.LocalDate
import io.circe.Encoder
import scala.util.matching.Regex

object CoreUtils {

  def extractDate(str: String): LocalDate = {
    val datePattern: Regex = """^.*(\d{4})[/-](\d{2})[/-](\d{2}).*$""".r
    str match {
      case datePattern(year, month, day) => LocalDate.of(year.toInt, month.toInt, day.toInt)
      case _ => throw new IllegalArgumentException(s"Unable to find Date in [$str]")
    }
  }

  def validateField[T, V](fld: T, validator: T => Boolean, fallbackValue: T): (T, Boolean) =
    Try((fld, if (validator(fld)) true else throw new Exception)) getOrElse (fallbackValue, false)

  def validateField[T](fld: Option[T], validator: T => Boolean, fallbackValue: T): (T, Boolean) =
    fld match {
      case Some(z) => Try((z, if (validator(z)) true else throw new Exception)) getOrElse (fallbackValue, false)
      case None => (fallbackValue, true)
    }

  def transformField[T, V](fld: T, transformer: T => V, fallbackValue: V): (V, Boolean) =
    Try(transformer(fld), true) getOrElse (fallbackValue, false)

  def transformField[T, V](fld: Option[T], transformer: T => V, fallbackValue: V): (V, Boolean) =
    fld match {
      case Some(z) => Try(transformer(z), true) getOrElse (fallbackValue, false)
      case None => (fallbackValue, true)
    }

  def printJSON[T](tObj: T)(implicit enc: Encoder[T]): String = {
    import io.circe.syntax._
    tObj.asJson(enc).toString
  }

  def prettyPathFiles(paths: String): String = {
    val inputPaths: Array[String] = paths.split(",")
    val sampleUrl = inputPaths(0)
    val embeddedDate: String = Try(extractDate(sampleUrl).toString.replace("-", "/")) getOrElse ""
    val stemPath = sampleUrl.take(sampleUrl.indexOfSlice(embeddedDate))
    val groupPartition: String = inputPaths.map(x => Try(extractDate(x).toString) getOrElse x).mkString("{", ",", "}")
    stemPath + groupPartition
  }

}
