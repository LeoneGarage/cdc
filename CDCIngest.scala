// Databricks notebook source
val user = dbutils.notebook.getContext().userName.get
val bronze_path = s"/Users/${user}/tmp/cdc/bronze/data"
val silver_path = s"/Users/${user}/tmp/cdc/silver/data"
val silver_checkpoint_path = s"/Users/${user}/tmp/cdc/silver/cp"

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
// MAGIC bronze_path = f'/Users/{user}/tmp/cdc/bronze/data'

// COMMAND ----------

// dbutils.fs.rm(s"${silver_path}", true)

// COMMAND ----------

import org.apache.spark.sql.execution.streaming._
import org.apache.hadoop.fs._
import com.databricks.sql.transaction.tahoe.sources.DeltaSourceOffset // for OSS Delta use import org.apache.spark.sql.delta.sources.DeltaSourceOffset
import com.databricks.sql.transaction.tahoe.DeltaLog // for OSS Delta use org.apache.spark.sql.delta.DeltaLog
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import scala.annotation.tailrec
import org.apache.hadoop.fs.FileUtil

case class Version(version: Long, operation: String)

class Checkpoint(checkpointDir: String, backupSubdir: String) extends CommitLog(spark, new Path(new Path(new Path(checkpointDir), backupSubdir), "commits").toString) {
  private val sourceCommitLog = new CommitLog(spark, new Path(new Path(checkpointDir), "commits").toString)

  def moveLastCommits(): CommitLog = {
    val fs = new Path(checkpointDir).getFileSystem(conf)
    val backupPath = new Path(new Path(checkpointDir), backupSubdir)
    val checkpointPath = new Path(checkpointDir)
    sourceCommitLog.getLatestBatchId().map(batchId => {
      if( fileManager.exists(new Path(checkpointPath, "metadata")) ) {
        fileManager.delete(new Path(backupPath, "metadata"))
        FileUtil.copy(fs, new Path(checkpointPath, "metadata"), fs, backupPath, /*deleteSource = */false, /*overwrite = */true, conf)
      }
      if( fileManager.exists(new Path(checkpointPath, "commits")) ) {
        fileManager.delete(new Path(backupPath, "commits"))
        FileUtil.copy(fs, new Path(checkpointPath, "commits"), fs, backupPath, /*deleteSource = */true, /*overwrite = */true, conf)
      }
      if( fileManager.exists(new Path(checkpointPath, "offsets")) ) {
        fileManager.delete(new Path(backupPath, "offsets"))
        FileUtil.copy(fs, new Path(checkpointPath, "offsets"), fs, backupPath, /*deleteSource = */true, /*overwrite = */true, conf)
      }
    })
    this
  }
}

// Read Delta Streaming Checkpoint and retrieve the last committed Delta version
object StreamingDeltaCheckpoint {
  private def retrieveDeltaVersion(tableNameOrPath: String, offsetsPath: Path, commitLog: CommitLog): Option[DeltaSourceOffset] = {
    commitLog.getLatest() match {
      case Some((batchId, _)) => {
        val tableId = DeltaLog.forTable(spark, tableNameOrPath).tableId
        val offsetSeq = new OffsetSeqLog(spark, offsetsPath.toString)
        offsetSeq.get(batchId).map(ofs => ofs.offsets.flatMap(o => o.map(dof => DeltaSourceOffset(tableId, dof.asInstanceOf[Offset])))) match {
          case Some(Seq(dos)) => Some(dos)
          case _ => None
        }
      }
      case None => None
    }
  }

  def getDeltaVersion(tableNameOrPath: String, checkpointDir: String): Option[DeltaSourceOffset] = {
    retrieveDeltaVersion(tableNameOrPath, new Path(new Path(checkpointDir), "offsets"), new CommitLog(spark, new Path(new Path(checkpointDir), "commits").toString))
  }

  def getDeltaVersionAndMoveLastCommits(tableNameOrPath: String, checkpointDir: String): Option[DeltaSourceOffset] = {
    retrieveDeltaVersion(tableNameOrPath, new Path(new Path(new Path(checkpointDir), "backup"), "offsets"), new Checkpoint(checkpointDir, "backup").moveLastCommits())
  }
}

// Use VERSION AS OF to construct a change set DataFrame of all the the changes between the latest Delta version and the version in Streaming checkpoint
object InsertCDC {
  private def unionAndExecuteDataFrames(emptyDf: DataFrame, group: Iterator[DataFrame]): DataFrame = {
    val gdf = group.foldLeft(emptyDf)(_ union _).persist(StorageLevel.MEMORY_AND_DISK)
    gdf.write.format("noop").mode("overwrite").save()
    gdf
  }
  
  @tailrec private def groupUnionDataFrames(emptyDf: DataFrame, frames: Iterator[DataFrame]): DataFrame = {
    var count: Int = 0
    val groupOfUnions = frames.grouped(30).map(g => {
      count = count + 1
      unionAndExecuteDataFrames(emptyDf, g.toIterator)
    })
    if( count > 1) {
      groupUnionDataFrames(emptyDf, groupOfUnions)
    }
    else {
      groupOfUnions.foldLeft(emptyDf)(_ union _)
    }
  }

  def getLatestDeltaVersion(tableName: Option[String],
                            tablePath: Option[String]): Long = {
    val tableNameOrPath = tableName.getOrElse(tablePath.get)
    val tableRef = tableName.getOrElse(s"delta.`${tablePath.get}`")
    spark.sql(s"DESCRIBE HISTORY ${tableRef} LIMIT 1").select("version").as[Long].collect().headOption.getOrElse(0L)
  }

  def getChangesSinceLastCheckpoint(tableName: Option[String],
                                    tablePath: Option[String],
                                    checkpointDir: String,
                                    partitionFilter: String): DataFrame = {
    val tableNameOrPath = tableName.getOrElse(tablePath.get)
    val tableRef = tableName.getOrElse(s"delta.`${tablePath.get}`")
    val latestVersion = getLatestDeltaVersion(tableName, tablePath)
    val lastCheckpointVersion = StreamingDeltaCheckpoint.getDeltaVersionAndMoveLastCommits(tableNameOrPath, checkpointDir).fold(0L)(_.reservoirVersion - 1)

    val versions = spark.sql(s"DESCRIBE HISTORY ${tableRef} LIMIT ${latestVersion - lastCheckpointVersion}").select("version", "operation").as[Version]
                        .collect()

    val opts = versions.filter(_.operation == "OPTIMIZE")
    val cdcDeltas = versions.indices.foldLeft(ArrayBuffer.empty[Version])((a, i) => if((i == 0 || versions(i-1).operation == "OPTIMIZE") && versions(i).operation != "OPTIMIZE") a :+ versions(i) else a )

    val emptyInsertsDf = spark.sql(s"SELECT *, input_file_name() as file_name FROM ${tableRef} WHERE 1=0")
    val emptyOptimizeDf = spark.sql(s"SELECT DISTINCT input_file_name() as file_name FROM ${tableRef} WHERE 1=0")

    val insertsDf = groupUnionDataFrames(emptyInsertsDf, cdcDeltas.toIterator.map(v => spark.sql(s"SELECT *, input_file_name() as file_name FROM ${tableRef} VERSION AS OF ${v.version} ${partitionFilter}")))
                             .join(spark.sql(s"SELECT DISTINCT input_file_name() as file_name FROM ${tableRef} VERSION AS OF $lastCheckpointVersion ${partitionFilter}"), Seq("file_name"), "left_anti")

    val optimizeDf = groupUnionDataFrames(emptyOptimizeDf, opts.toIterator.map(o => {
                                      val q = spark.sql(s"SELECT DISTINCT input_file_name() as file_name FROM ${tableRef} VERSION AS OF ${o.version} ${partitionFilter}")
                                      if ((o.version - 1) > lastCheckpointVersion) {
                                        q.join(spark.sql(s"SELECT DISTINCT input_file_name() as file_name FROM ${tableRef} VERSION AS OF ${o.version-1} ${partitionFilter}"), Seq("file_name"), "left_anti")
                                      }
                                      else
                                      {
                                        q
                                      }
                                   }
                             ))

    insertsDf.join(optimizeDf, Seq("file_name"), "left_anti").drop("file_name")
  }
}

// COMMAND ----------

// An example of how to access Delta version from checkpoint
StreamingDeltaCheckpoint.getDeltaVersion(bronze_path, silver_checkpoint_path).get.reservoirVersion

// COMMAND ----------

// dbutils.fs.rm(bronze_path, true)

// COMMAND ----------

spark.sql(s"""
CREATE TABLE IF NOT EXISTS delta.`${bronze_path}` (
  address STRING,
  email STRING,
  firstname STRING,
  lastname STRING,
  customer_operation STRING,
  customer_operation_date STRING,
  amount STRING,
  customer_id STRING,
  transaction_id STRING,
  item_count STRING,
  operation STRING,
  operation_date STRING,
  transaction_date STRING,
  date INT)
USING delta
PARTITIONED BY (date)
LOCATION '${bronze_path}'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '4')
""")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # INSERT some rows into bronze source tables
// MAGIC 
// MAGIC from datetime import datetime
// MAGIC import uuid
// MAGIC import random
// MAGIC from pyspark.sql.functions import desc, to_timestamp, lit
// MAGIC from concurrent.futures import ThreadPoolExecutor
// MAGIC import concurrent.futures
// MAGIC 
// MAGIC numRows = 20
// MAGIC path = bronze_path
// MAGIC timePattern = "%m-%d-%Y %H:%M:%S"
// MAGIC timeNow = datetime.today().strftime(timePattern)
// MAGIC with ThreadPoolExecutor(16*20) as executor:
// MAGIC   futures = []
// MAGIC   for i in range(0, numRows):
// MAGIC     task = lambda: spark.sql(f"INSERT INTO delta.`{path}` VALUES('Unit 8168 Box 1926\nDPO AP 57543','taylor08@odonnell.com','Steven','Marshall',null,'{datetime.today().strftime(timePattern)}',{round(random.random()*10000.0)/100.0},'{str(uuid.uuid4())}','{str(uuid.uuid4())}',2.0,null,'{datetime.today().strftime(timePattern)}','{datetime.today().strftime(timePattern)}',20220300)")
// MAGIC     futures.append(executor.submit(task))
// MAGIC   concurrent.futures.wait(futures)
// MAGIC 
// MAGIC display(spark.read.format('delta').load(path).where(to_timestamp('customer_operation_date', 'MM-dd-yyyy HH:mm:ss') >= to_timestamp(lit(timeNow), 'MM-dd-yyyy HH:mm:ss')).orderBy(desc("customer_operation_date")))

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC spark.sql(f'OPTIMIZE delta.`{path}`')

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val checkpointDir = silver_checkpoint_path
val sourceTablePath = bronze_path

def createUpdateSilver() = {
  val cdcDf = InsertCDC.getChangesSinceLastCheckpoint(None,
                                                    Some(sourceTablePath),
                                                    checkpointDir,
                                                    "WHERE date = 20220300")
  def updateSilver(batchDf: DataFrame, batchId: Long) = {
     cdcDf.write.format("delta").mode("append").save(silver_path)
  }
  updateSilver _
}

val latestVersion = InsertCDC.getLatestDeltaVersion(None,
                                                    Some(sourceTablePath))

// prepare to advance checkpoint
val inDf = spark.readStream
           .option("readChangeFeed", "true")
           .option("startingVersion", Math.max(0L, latestVersion-1L)) // Use version one before latest in case latest is OPTIMIZE operation, which results ion no microbatches coming through
           .format("delta")
           .load(sourceTablePath)
           .where("date = 20220300 and 1=0") // Ignore any microbatch data since we are going to force a change set by using InsertCDC.getChangesSinceLastCheckpoint() call. We only want a foreachBatch() callback to be made

// advance checkpoint
val queryStream = inDf.writeStream.format("noop")
    .option("checkpointLocation", checkpointDir)
    .foreachBatch(createUpdateSilver())
    .trigger(Trigger.Once)
    .start()

// COMMAND ----------

spark.read.format("delta").load(silver_path).count()

// COMMAND ----------

import org.apache.spark.sql.functions.col

display(spark.read.format("delta").load(silver_path).orderBy(col("customer_operation_date").desc))

// COMMAND ----------

display(spark.sql(s"DESCRIBE HISTORY delta.`${silver_path}`"))
