package com.analytic.entities.adapters

import org.scalatest.flatspec.AnyFlatSpec
import com.analytic.entities.models.TableReference
import com.analytic.entities.models.TableLoadType.TableLoadType
import java.time.LocalDate

import com.analytic.entities.exceptions.RepositoryException
import com.analytic.entities.models.{TableLoadType, TablePartition, TableReference}
import com.analytic.entities.ports.TTableRepository

class TableRepositoryTest extends AnyFlatSpec {
  val testTSRepository: TTableRepository = new TableRepository with GoogleCloudStorageOperator
  val testProcessDate = LocalDate.of(2020,8,2)
  val testInputBucket = "generic-entity-dummy-test-033eb204a2e06238"

  "generateRepository" should "throw RepositoryException if no url matches" in {
    val testTables: List[TableReference] = List(
      TableReference(
        tableId = "fake-table",
        regexUrl = """^(fake-path/cl/fake-sid/fake-schema/fake-table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "fake-path/cl/fake-sid/fake-schema/fake-table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.INTERVAL
      )
    )
    assertThrows[RepositoryException] {
      testTSRepository.generateRepository(testTables)
    }
  }
  it should "throw RepositoryException if no table matches pattern of regexURL [orcl schema]" in {
    val testTables: List[TableReference] = List(
      TableReference(
        tableId = "fake-table",
        regexUrl = """^(fake-path/fake-table/data/\d{4}/\d{2}/\d{2})/[-\._a-z0-9]+\.avro$""".r,
        stemUrl = "fake-path/cl/fake-sid/fake-schema/fake-table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.INTERVAL
      )
    )
    assertThrows[RepositoryException] {
      testTSRepository.generateRepository(testTables)
    }
  }
  it should "throw RepositoryException if no table matches pattern of regexURL [adobe schema]" in {
    val testTables: List[TableReference] = List(
      TableReference(
        tableId = "fake-table",
        regexUrl = """^(fake-path/fake-table/cl/\d{4}/\d{2}/\d{2}/[-._a-z0-9]+/data)/[-._a-z0-9]+\.tsv$""".r,
        stemUrl = "fake-path/fake-table/cl/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.INTERVAL
      )
    )
    assertThrows[RepositoryException] {
      testTSRepository.generateRepository(testTables)
    }
  }
  it should "return the correct list of TableRepository [orcl schema]" in {
    val testTargetTables: List[TableReference] = List(
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.MOST_RECENT
      )
    )
    val expectedRepoLength = 4

    val actualRepo: List[TablePartition] = testTSRepository.generateRepository(testTargetTables)
    assertResult(expectedRepoLength)(actualRepo.length)
  }
  it should "return the correct list of TableRepository [adobe schema]" in {
    val testTargetTables: List[TableReference] = List(
      TableReference(
        tableId = "table",
        regexUrl = """^(adobe/table/cl/\d{4}/\d{2}/\d{2}/[-._a-z0-9]+/data)/[-._a-z0-9]+\.tsv$""".r,
        stemUrl = "adobe/table/cl/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.MOST_RECENT
      )
    )
    val expectedRepoLength = 23

    val actualRepo: List[TablePartition] = testTSRepository.generateRepository(testTargetTables)
    assertResult(expectedRepoLength)(actualRepo.length)
  }

  "matchParseURL" should "return the correct TablePartition [orcl schema]" in {
    val expectedTable = TablePartition(
      tableId = "table",
      partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/02",
      hostBucket = testInputBucket
    )
    val actualTable = testTSRepository.matchParseURL(
      url = "orcl/cl/sid/schema/table/data/2020/08/02/schema.table.2020_08_02_part_0000.avro",
      regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
      tableId = "table",
      hostBucket = testInputBucket
    )
    assert(actualTable.isDefined)
    assertResult(expectedTable)(actualTable.get)
  }
  it should "return the correct TablePartition [adobe schema]" in {
    val expectedTable = TablePartition(
      tableId = "table",
      partitionUrl = "adobe/table/cl/2020/08/02/00/data",
      hostBucket = testInputBucket
    )
    val actualTable = testTSRepository.matchParseURL(
      url = "adobe/table/cl/2020/08/02/00/data/01-datacl_20200802-000000.tsv",
      regexUrl = """^(adobe/table/cl/\d{4}/\d{2}/\d{2}/[-._a-z0-9]+/data)/[-._a-z0-9]+\.tsv$""".r,
      tableId = "table",
      hostBucket = testInputBucket
    )
    assert(actualTable.isDefined)
    assertResult(expectedTable)(actualTable.get)
  }
  it should "return None on nonMatching url pattern" in {
    val actual = testTSRepository.matchParseURL(
      url = "fake-path/cl/fake-schema/fake-table/data/2020/08/02/file.avro",
      regexUrl = """^(fake-path/cl/fake-sid/fake-schema/fake-table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
      tableId = "fake-table",
      hostBucket = testInputBucket
    )
    assert(actual.isEmpty)
  }

  "getPartitionURL" should "throw RepositoryException if testTSRepository is empty." in {
    val dummyTableReference = TableReference(
      tableId = "table",
      regexUrl = """^(adobe/table/cl/\d{4}/\d{2}/\d{2}/[-._a-z0-9]+/data)/[-._a-z0-9]+\.tsv$""".r,
      stemUrl = "adobe/table/cl/",
      processDate = testProcessDate,
      hostBucket = testInputBucket,
      loadType = TableLoadType.MOST_RECENT
    )
    assertThrows[RepositoryException] {
      testTSRepository.getPartitionURL(List())(dummyTableReference)
    }
  }
  it should "throw RepositoryException if partition for DAILY table is not found in repository." in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate.plusDays(3),
        hostBucket = testInputBucket,
        loadType = TableLoadType.INTERVAL
    )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/02",
        hostBucket = testInputBucket
      )
    )
    assertThrows[RepositoryException] {
      testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    }
  }
  it should "throw RepositoryException if partition for ALL_DATA table is not found in repository." in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.ALL_DATA
      )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/03",
        hostBucket = testInputBucket
      )
    )
    assertThrows[RepositoryException] {
      testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    }
  }

  it should "throw RepositoryException if partition for INTERVAL table is not found in repository." in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.INTERVAL,
        partitionsDays = 12
      )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/03",
        hostBucket = testInputBucket
      )
    )
    assertThrows[RepositoryException] {
      testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    }
  }
  it should "throw RepositoryException if target table load type is NA" in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(fake-path/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "fake-path/cl/sid/schema/table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.INTERVAL,
        partitionsDays = 12
      )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/03",
        hostBucket = testInputBucket
      )
    )
    assertThrows[RepositoryException] {
      testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    }
  }
  it should "return an INTERVAL table partition at process date url if found in repository." in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.INTERVAL
      )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/02",
        hostBucket = testInputBucket
      )
    )
    val url = testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    val expectedURL = testRepo(0).fullUrl
    assertResult(expectedURL)(url)
  }
  it should "return MOST_RECENT table url if a valid partition is found in repository." in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.MOST_RECENT
      )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/02",
        hostBucket = testInputBucket
      )
    )
    val url = testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    val expectedURL = testRepo(0).fullUrl
    assertResult(expectedURL)(url)
  }
  it should "return all files URL if loadTypeS is ALL_DATA" in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.ALL_DATA
      )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/02",
        hostBucket = testInputBucket
      )
    )
    val url = testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    val expectedURL = testRepo(0).fullUrl
    assertResult(expectedURL)(url)
  }
  it should "return partitions URLs within the partitionsDays interval if loadTypeS is INTERVAL" in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.INTERVAL,
        partitionsDays = 3
      )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/07/31",
        hostBucket = testInputBucket
      ),
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/01",
        hostBucket = testInputBucket
      ),
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/02",
        hostBucket = testInputBucket
      )
    )
    val url = testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    val expectedURL = testRepo(0).fullUrl + "," +
      testRepo(1).fullUrl + "," +
      testRepo(2).fullUrl
    assertResult(expectedURL)(url)
  }
  it should "return all partitions URLs with matching coreURL if loadTypeS is TOTAL" in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate,
        hostBucket = testInputBucket,
        loadType = TableLoadType.TOTAL
      )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/07/30",
        hostBucket = testInputBucket
      ),
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/07/31",
        hostBucket = testInputBucket
      ),
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/01",
        hostBucket = testInputBucket
      ),
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/02",
        hostBucket = testInputBucket
      )
    )
    val url = testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    val expectedURL = testRepo(0).fullUrl + "," +
      testRepo(1).fullUrl + "," +
      testRepo(2).fullUrl + "," +
      testRepo(3).fullUrl
    assertResult(expectedURL)(url)
  }
  it should "return all partitions URLs with matching coreURL if loadTypeS is MOST_RECENT_UNTIL" in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate.plusDays(-3),
        hostBucket = testInputBucket,
        loadType = TableLoadType.MOST_RECENT_UNTIL
      )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/07/30",
        hostBucket = testInputBucket
      )
    )
    val url = testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    val expectedURL = testRepo(0).fullUrl
    assertResult(expectedURL)(url)
  }
  it should "throw RepositoryException if no loadType matches" in {
    val testTargetTables: TableReference =
      TableReference(
        tableId = "table",
        regexUrl = """^(orcl/cl/sid/schema/table/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "orcl/cl/sid/schema/table/data/",
        processDate = testProcessDate.plusDays(-3),
        hostBucket = testInputBucket,
        loadType = null
      )
    val testRepo: List[TablePartition] = List(
      TablePartition(
        tableId = "table",
        partitionUrl = "orcl/cl/sid/schema/table/data/2020/08/02",
        hostBucket = testInputBucket
      )
    )
    assertThrows[RepositoryException] {
      testTSRepository.getPartitionURL(testRepo)(testTargetTables)
    }
  }
}
