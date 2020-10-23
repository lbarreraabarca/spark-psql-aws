package com.analytic.entities.adapters

import com.analytic.entities.exceptions.CloudStorageException
import org.scalatest.flatspec.AnyFlatSpec

class GoogleCloudStorageOperatorTest extends AnyFlatSpec with GoogleCloudStorageOperator {
  "downloadBlob" should "throw StorageBucketException if GCS file is unreachable." in {
    val unreachableFile = "null-bkt/file_043629202"
    assertThrows[CloudStorageException] {
      downloadBlob(unreachableFile, "/tmp/null")
    }
  }
  it should "throw StorageBucketException if blobURL is invalid" in {
    val invalidFile = "invalid-file_043629202"
    assertThrows[CloudStorageException] {
      downloadBlob(invalidFile, "/tmp/null")
    }
  }
  it should "throw StorageBucketException if targetDir is unreachable" in {
    val blob = "config-datalake-test/orcl/cl/asrx_cdlv/srxfa/dtk_vtadotwf/schema/avro/srxfa.dtk_vtadotwf.avsc"
    assertThrows[CloudStorageException] {
      downloadBlob(blob, "/")
    }
  }
  it should "throw StorageBucketException if targetDir is null" in {
    val blob = "config-datalake-test/orcl/cl/asrx_cdlv/srxfa/dtk_vtadotwf/schema/avro/srxfa.dtk_vtadotwf.avsc"
    val targetDir = null
    assertThrows[CloudStorageException] {
      downloadBlob(blob, targetDir)
    }
  }
  it should "throw StorageBucketException if targetDir is empty" in {
    val blob = "config-datalake-test/orcl/cl/asrx_cdlv/srxfa/dtk_vtadotwf/schema/avro/srxfa.dtk_vtadotwf.avsc"
    val targetDir = ""
    assertThrows[CloudStorageException] {
      downloadBlob(blob, targetDir)
    }
  }
  it should "throw StorageBucketException if blob is empty" in {
    val blob = ""
    val targetDir = "/tmp/file"
    assertThrows[CloudStorageException] {
      downloadBlob(blob, targetDir)
    }
  }
  it should "throw StorageBucketException if blob is null" in {
    val blob = null
    val targetDir = "/tmp/file"
    assertThrows[CloudStorageException] {
      downloadBlob(blob, targetDir)
    }
  }
  it should "download a blob into the given local path" in {
    import scala.io.Source
    val blob = "config-datalake-test/orcl/cl/asrx_cdlv/srxfa/dtk_vtadotwf/schema/avro/srxfa.dtk_vtadotwf.avsc"
    downloadBlob(blob, "/tmp/null")
    val fileContent = Source.fromFile("/tmp/null").getLines.mkString("\n")
    assert(fileContent.startsWith("""{"type":"record""""))
  }

  "listBlobs" should "list the blobs in a bucket with an specific URL" in {
    val bkt = "datalake_retail_f0e87b52-b4b1-425b-a804-b40b09f11630"
    val prefix = "unit_tests_data/"
    val list = listBlobs(bkt, prefix)
    assert(list.nonEmpty)
  }
  it should "return an empty iterable if prefix does not match anything" in {
    val bkt = "config-datalake-test"
    val prefix = "weirdprefix/"
    val list = listBlobs(bkt, prefix)
    assertResult(0)(list.size)
  }
  it should "throw StorageBucketException if bucket is unreachable" in {
    val invalidBkt = "unexistent-bkt-3254325"
    assertThrows[CloudStorageException] {
      listBlobs(invalidBkt, "")
    }
  }
  it should "throw StorageBucketException if bucket is null" in {
    val invalidBkt = null
    assertThrows[CloudStorageException] {
      listBlobs(invalidBkt, "")
    }
  }
  it should "throw StorageBucketException if bucket is empty" in {
    val invalidBkt = ""
    assertThrows[CloudStorageException] {
      listBlobs(invalidBkt, "")
    }
  }
  it should "throw StorageBucketException if prefix is null" in {
    assertThrows[CloudStorageException] {
      listBlobs("bkt", null)
    }
  }

  "readFile" should "throw CloudStorageException if file not found." in {
    assertThrows[CloudStorageException] {
      readFile("src/test/resources/unexistent.yaml")
    }
  }
  it should "throw CloudStorageException if path is empty." in {
    assertThrows[CloudStorageException] {
      readFile("")
    }
  }
  it should "copy a public GCS file to local and return its content if filename is a GCS URL." in {
    val publicGCSFile: String =
      "gs://config-datalake-test/orcl/cl/asrx_cdlv/srxfa/dtk_vtadotwf/schema/avro/srxfa.dtk_vtadotwf.avsc"
    val fileContent: String = readFile(publicGCSFile)
    assert(fileContent.startsWith("""{"type":"record""""))
  }

}
