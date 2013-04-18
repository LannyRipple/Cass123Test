package com.spotright.cass123test

import scala.collection.JavaConverters._

import java.nio.ByteBuffer

import org.apache.cassandra.db.IColumn
import org.apache.cassandra.hadoop.{ColumnFamilyInputFormat, ConfigHelper}
import org.apache.cassandra.thrift.{SliceRange, SlicePredicate}

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.apache.hadoop.util.{Tool, ToolRunner, GenericOptionsParser}

object LooperHelper {
  type OurMapperType = Mapper[ByteBuffer, java.util.SortedMap[ByteBuffer, IColumn], NullWritable, NullWritable]
}

import LooperHelper.OurMapperType

class LooperMapper extends OurMapperType {

  override
  def map(key: ByteBuffer, columns: java.util.SortedMap[ByteBuffer, IColumn], context: OurMapperType#Context) {
    context.getCounter("Totals", "Row Count").increment(1L)
    context.getCounter("Totals", "Column Count").increment(columns.size.toLong)
  }
}

class Looper extends Configured with Tool {
  def run(args: Array[ String ]): Int = {
    val conf = getConf
    new GenericOptionsParser(conf, args)
    val job = jobFactory(conf)

    if (job.waitForCompletion(true)) 0 else 1
  }

  def processArgs(conf: Configuration, av: Array[ String ]) {
    new GenericOptionsParser(conf, av).getRemainingArgs
  }

  def jobFactory(conf: Configuration): Job = {
    val job = new Job(conf, "looper count")

    job.setJarByClass(classOf[ LooperMapper ])

    job.setMapperClass(classOf[ LooperMapper ])

    job.setOutputKeyClass(classOf[ NullWritable ])
    job.setOutputValueClass(classOf[ NullWritable ])

    job.setInputFormatClass(classOf[ ColumnFamilyInputFormat ])

    job.setNumReduceTasks(0)

    FileOutputFormat.setOutputPath(job, new Path("" + (System.currentTimeMillis() / 1000) +  "-looper"))

    cassConfig(job)

    job
  }

  def cassConfig(job: Job) {
    val conf = job.getConfiguration()

    ConfigHelper.setInputRpcPort(conf, "" + 9160)
    ConfigHelper.setInputInitialAddress(conf, Config.hostip)

    ConfigHelper.setInputPartitioner(conf, "org.apache.cassandra.dht.RandomPartitioner")
    ConfigHelper.setInputColumnFamily(conf, Config.keyspace, Config.cfname)

    val pred = {
      val range = new SliceRange()
        .setStart("".getBytes("UTF-8"))
        .setFinish("".getBytes("UTF-8"))
        .setReversed(false)
        .setCount(4096 * 1000)

      new SlicePredicate().setSlice_range(range)
    }

    ConfigHelper.setInputSlicePredicate(conf, pred)
  }
}

object Looper {

  def main(args: Array[String]) {
    val res: Int = ToolRunner.run(new Looper, args)
    System.exit(res)
  }
}
