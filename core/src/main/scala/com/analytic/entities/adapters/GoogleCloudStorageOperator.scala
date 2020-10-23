package com.analytic.entities.adapters

import com.analytic.entities.exceptions.CloudStorageException
import com.analytic.entities.ports.CloudStorageOperator
import com.typesafe.scalalogging.Logger

import scala.util.{Failure, Success, Try}
import scala.io.{BufferedSource, Source}
import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Blob, BlobId, Storage, StorageOptions}

trait GoogleCloudStorageOperator extends CloudStorageOperator {
  private val log: Logger = Logger(classOf[GoogleCloudStorageOperator])

  private def getStorageService: Storage = StorageOptions.getDefaultInstance.getService

	def listBlobs(hostBucket: String, prefix: String = ""): Iterable[String] = {
    import scala.collection.JavaConverters._
    if (hostBucket == null || hostBucket.trim.isEmpty) throw new CloudStorageException(s"hostBucket cannot be null nor empty")
    else if (prefix == null) throw new CloudStorageException("prefix cannot be null")
    else try {
      val blobs: Page[Blob] = getStorageService.list(hostBucket, Storage.BlobListOption.prefix(prefix))
      blobs.iterateAll.asScala.map(_.getName)
    } catch {
      case x: Exception => throw new CloudStorageException(
        s"Unable to list blobs from bucket $hostBucket with prefix $prefix - ${x.getClass}: ${x.getMessage}"
      )
    }
  }

  def downloadBlob(blobURL: String, targetDir: String): Unit = {
    import java.nio.file.Paths
    if (blobURL == null || blobURL.trim.isEmpty) throw new CloudStorageException(s"blobURL cannot be null nor empty")
    else if (targetDir == null || targetDir.trim.isEmpty) throw new CloudStorageException(s"targetDir cannot be null nor empty")
    else {
      val decomposedURL: List[String] = blobURL.split("/").toList
      val (bktName, blobDir): (String, String) = decomposedURL match {
        case bkt :: blb => (bkt, blb.mkString("/"))
        case _ => throw new CloudStorageException(s"Invalid file URL: $blobURL")
      }
      val targetPath = Paths.get(targetDir)
      try {
        log.info(s"Reaching GCS file gs://$blobURL")
        val blob: Blob = getStorageService.get(BlobId.of(bktName, blobDir))
        blob.downloadTo(targetPath)
      } catch {
        case x: Exception => throw new CloudStorageException(s"Unable to download blob - ${x.getClass}: ${x.getMessage}")
      }
    }
  }

  def readFile(path: String): String = {
    if (path == null || path.trim.isEmpty) throw new CloudStorageException("path cannot be null nor empty")
    else {
      val filename: String = if (path.startsWith("gs://")) {
        val tmpFile = "/tmp/" + java.util.UUID.randomUUID.toString
        downloadBlob(path.drop(5), tmpFile)
        tmpFile
      } else path
      log.info(s"Reading local file $filename")
      val bufferSrc: Try[BufferedSource] = Try(Source.fromFile(filename, "UTF-8"))
      try bufferSrc match {
        case Success(b) => b.getLines.mkString("\n")
        case Failure(ex) => throw ex
      } catch {
        case x: Exception => throw new CloudStorageException(s"Unable to read file - ${x.getClass}: ${x.getMessage}")
      } finally Try(bufferSrc.get.close())
    }
  }

}
