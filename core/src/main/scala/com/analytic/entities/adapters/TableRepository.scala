package com.analytic.entities.adapters

import com.analytic.entities.exceptions.RepositoryException
import com.analytic.entities.models.{TableLoadType, TablePartition, TableReference}
import com.analytic.entities.ports.{CloudStorageOperator, TTableRepository}
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import scala.collection.immutable.Set


class TableRepository extends TTableRepository { self: CloudStorageOperator =>
  private val log: Logger = Logger(classOf[TableRepository])

  def generateRepository(targetTables: List[TableReference]): List[TablePartition] = {
    val missingTables: ListBuffer[String] = ListBuffer()
    def getTableRepo(ts: TableReference): List[TablePartition] = {
      val blobsList: Iterable[String] = listBlobs(ts.hostBucket, ts.stemUrl)
      val parsedTables: Set[TablePartition] = blobsList.flatMap(matchParseURL(_, ts.regexUrl, ts.tableId, ts.hostBucket)).toSet
      if (parsedTables.isEmpty) missingTables += s"${ts.stemUrl} @ ${ts.hostBucket}"
      parsedTables.toList
    }
    val repository: List[TablePartition] = for (table <- targetTables; repoI <- getTableRepo(table)) yield repoI
    log.info(s"Repository contains ${repository.length} tables")
    if (missingTables.nonEmpty) throw new RepositoryException(s"No matches found on ${missingTables.mkString("|")}")
    else repository
  }

  def matchParseURL(url: String, regexUrl: Regex, tableId: String, hostBucket: String): Option[TablePartition] = url match {
    case regexUrl(partitionUrl) => Some(TablePartition(tableId, partitionUrl, hostBucket))
    case _ => None
  }

  private def getPartitions: (List[TablePartition], TableReference)  => List[TablePartition] = (repository: List[TablePartition], target: TableReference) => {
    target.loadType match {
      case TableLoadType.TOTAL => repository.filter(_.tableId == target.tableId)
      case TableLoadType.ALL_DATA => repository.filter { ts =>
        ts.tableId == target.tableId && (ts.partitionDate.isEqual(target.partitionDate) || ts.partitionDate.isBefore(target.partitionDate))
      }
      case TableLoadType.MOST_RECENT => repository.filter(_.tableId == target.tableId).maxBy(_.partitionDate.toEpochDay) :: Nil
      case TableLoadType.INTERVAL => repository.filter { ts =>
        ts.tableId == target.tableId && ( ts.partitionDate.isEqual(target.partitionDate) ||
          (ts.partitionDate.isBefore(target.partitionDate) && ts.partitionDate.isAfter(target.partitionDateMin)) )
      }
      case TableLoadType.MOST_RECENT_UNTIL => repository.filter { ts =>
        ts.tableId == target.tableId && (ts.partitionDate.isEqual(target.partitionDate) || ts.partitionDate.isBefore(target.partitionDate))
      }.maxBy(_.partitionDate.toEpochDay) :: Nil
      case _ => throw new RepositoryException(s"Invalid load type")
    }
  }

  def getPartitionURL(repository: List[TablePartition])(targetTable: TableReference): String = {
    if (repository.isEmpty) throw new RepositoryException(s"Repository is empty")
    else {
      val partitionS: List[TablePartition] = getPartitions(repository, targetTable)
      if (partitionS.nonEmpty) partitionS.sortBy(_.partitionDate.toEpochDay).map(_.fullUrl).mkString(",")
      else throw new RepositoryException(s"No partition found for table $targetTable")
    }
  }

}
