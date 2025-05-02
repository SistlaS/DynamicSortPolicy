import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

val EXPERIMENT_NAME = "exp-5k-5k-skew-2.0-1.1-10keys"
val KEY_COLUMN = "key"
val COUNT_COLUMN = "global_count"
val NUM_PARTITIONS = 4
val SPARK_MASTER_URL = "spark://your-master:7077"
val HDFS_LEFT_INPUT_PATH = "hdfs://nn:9000/data/left_table.csv"
val HDFS_RIGHT_INPUT_PATH = "hdfs://nn:9000/data/right_table.csv"
val OUTPUT_BASE = "hdfs://nn:9000/data/scala_joined_output"

object RangePartitionedSortMergeJoin {
  case class KeyRange(partitionId: Int, startKey: Int, endKey: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(s"Unified Partitioned Merge Join__my_experiment")
      .master(SPARK_MASTER_URL)
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.join.preferSortMergeJoin", "true")
      .config("spark.executor.memory", "25g")
      .config("spark.driver.memory", "25g")
      .getOrCreate()

    import spark.implicits._

    // Step 1: Read both CSVs
    val left = spark.read.option("header", "true").csv(HDFS_LEFT_INPUT_PATH)
      .select($"left_row_id", $"key".cast("int").as("key"), $"left_payload")
    val right = spark.read.option("header", "true").csv(HDFS_RIGHT_INPUT_PATH)
      .select($"right_row_id", $"key".cast("int").as("key"), $"right_payload")

    // Step 2: Compute key frequencies
    val keyFreqs = left.select("key").union(right.select("key"))
      .groupBy("key").count()
      .orderBy("key")
      .collect()

    val total = keyFreqs.map(_.getLong(1)).sum
    val idealPerPartition = total / NUM_PARTITIONS

    // Step 3: Manually construct key ranges based on frequency
    var currentSum = 0L
    var currentKeys = Seq.empty[Int]
    var partitions = Seq.empty[KeyRange]
    var pid = 0

    for (row <- keyFreqs) {
      val k = row.getInt(0)
      val c = row.getLong(1)
      currentSum += c
      currentKeys :+= k
      if (currentSum >= idealPerPartition && pid < NUM_PARTITIONS - 1) {
        partitions :+= KeyRange(pid, currentKeys.head, currentKeys.last)
        currentSum = 0
        currentKeys = Seq.empty[Int]
        pid += 1
      }
    }
    if (currentKeys.nonEmpty) {
      partitions :+= KeyRange(pid, currentKeys.head, currentKeys.last)
    }

    // Step 4: Define custom partitioner
    class KeyRangePartitioner(ranges: Seq[KeyRange]) extends Partitioner {
      override def numPartitions: Int = ranges.length
      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[Int]
        ranges.indexWhere(r => k >= r.startKey && k <= r.endKey) match {
          case -1 => 0 // fallback if not found
          case pid => pid
        }
      }
    }

    val partitioner = new KeyRangePartitioner(partitions)

    // Step 5: Convert to key-value RDDs and partition
    val leftRDD = left.rdd.map(row => (row.getInt(1), row))
      .partitionBy(partitioner)
      .mapPartitions(_.toList.sortBy(_._1).iterator, preservesPartitioning = true)

    val rightRDD = right.rdd.map(row => (row.getInt(1), row))
      .partitionBy(partitioner)
      .mapPartitions(_.toList.sortBy(_._1).iterator, preservesPartitioning = true)

    // Step 6: Perform sort-merge join
    val joinedRDD = leftRDD.join(rightRDD)

    // Step 7: Write per-partition output
    joinedRDD.map {
      case (key, (leftRow, rightRow)) =>
        s"${leftRow.getString(0)},${key},${leftRow.getString(2)},${rightRow.getString(0)},${rightRow.getString(2)}"
    }.mapPartitionsWithIndex { case (pid, iter) =>
      iter.map(line => s"$pid,$line")
    }.saveAsTextFile(OUTPUT_BASE)

    spark.stop()
  }
}
