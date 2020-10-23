package com.analytic.entities.ports

trait CloudStorageOperator {
  def listBlobs(bkt: String, prefix: String): Iterable[String]
  def downloadBlob(blobURL: String, dst: String): Unit
  def readFile(path: String): String
}
