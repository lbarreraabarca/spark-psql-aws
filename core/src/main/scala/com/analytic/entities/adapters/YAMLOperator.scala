package com.analytic.entities.adapters

import com.analytic.entities.exceptions.YAMLException
import com.analytic.entities.ports.{CloudStorageOperator, TYAMLOperator}
import com.typesafe.scalalogging.Logger
import io.circe.{Decoder => YAMLDecoder}
import com.analytic.entities.ports.CloudStorageOperator

class YAMLOperator extends TYAMLOperator { self: CloudStorageOperator =>
  import io.circe._, io.circe.generic.auto._
  private val log: Logger = Logger(classOf[YAMLOperator])

  def parseFileTo[T: YAMLDecoder](path: String): T = {
    if (path == null || path.trim.isEmpty) throw new YAMLException("path cannot be null nor empty")
    else {
      val strContent = readFile(path)
      parseTo[T](strContent)
    }
  }

  def parseTo[T: YAMLDecoder](strContent: String): T = {
    import cats.syntax.either._
    import io.circe.yaml.parser, io.circe.ParsingFailure
    if (strContent == null || strContent.trim.isEmpty) throw new YAMLException("String content cannot be null nor empty")
    else try {
      log.debug(s"Parsing the following YAML string:\n$strContent")
      val js = parser.parse(strContent)
      js.leftMap(err => err: Error)
        .flatMap(_.as[T])
        .valueOr(throw _)
    } catch {
      case e: Exception => throw new YAMLException(s"Unable to parse string - ${e.getClass}: ${e.getMessage}")
    }
  }

}
