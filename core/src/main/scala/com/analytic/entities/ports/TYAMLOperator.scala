package com.analytic.entities.ports

import io.circe.{Decoder => YAMLDecoder}

trait TYAMLOperator {
  def parseFileTo[T: YAMLDecoder](path: String): T
  def parseTo[T: YAMLDecoder](strContent: String): T
}
