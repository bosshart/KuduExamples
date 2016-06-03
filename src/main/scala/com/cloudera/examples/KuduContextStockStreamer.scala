package com.cloudera.examples


import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.kududb.ColumnSchema.ColumnSchemaBuilder
import org.kududb.client.KuduClient.KuduClientBuilder
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.client.shaded.com.google.common.collect.ImmutableList
import org.kududb.client._
import org.kududb.spark.kudu.KuduContext
import org.kududb.{ColumnSchema, Schema, Type}

import scala.util.Try


object KuduContextStockStreamer {

  val log = Logger.getRootLogger.setLevel(Level.ERROR)

  lazy val schema: Schema = {
    val columns = ImmutableList.of(
      new ColumnSchemaBuilder("ticket", Type.STRING).key(true).build(),
      new ColumnSchemaBuilder("timestamp", Type.STRING).key(true).build(),
      new ColumnSchemaBuilder("per", Type.STRING).key(false).build(),
      new ColumnSchemaBuilder("last", Type.STRING).key(false).build(),
      new ColumnSchemaBuilder("vol", Type.STRING).key(false).build())
    new Schema(columns)
  }


  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: StockStreamer <brokers> <topics> <kuduMaster> <tableName>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <kuduMasters> is a list of one or more Kudu masters
                            |  <tableName> is the name of the kudu table
                            |  <local> 'local' to run in local mode
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics, kuduMaster, tableName, local) = args
    val runLocal = local.equals("local")
    val sparkConf = new SparkConf().setAppName("Kudu StockStreamer")
    var ssc:StreamingContext = null
    if (runLocal) {
      println("Running Local")
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      sparkConfig.set("spark.io.compression.codec", "lzf")
      val sc = new SparkContext("local[4]", "SparkSQL on Kudu", sparkConfig)
      ssc = new StreamingContext(sc, Seconds(2))
    } else {
      println("Running Cluster")
      ssc = new StreamingContext(sparkConf, Seconds(2))
    }
    val kuduContext = new KuduContext(kuduMaster)
    /**val sparkContext = new SparkContext(sparkConf)
    val sqLContext = new SQLContext(sparkContext)

    val tableOptions: Map[String, String] = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> kuduMaster)

    val scanRdd = kuduContext.kuduRDD(sparkContext, "test", Seq("key"))
    val df = sqLContext.read.options(tableOptions).format("org.kududb.spark.kudu").load
    **/
    //createTable(kuduClient, kuduMaster, tableName)

    val schemaString = "ticket per date time last vol"
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "spark.streaming.kafka.maxRetries" -> "5")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)


    val parsedTickLines = messages.transform { rdd =>
      val parsed = rdd.map(tickLine => (TickEventBuilder.build(tickLine._2)))
      parsed.foreachPartition(it => {
        val pClient = kuduContext.syncClient //new KuduClientBuilder(kuduMaster).build()
        val table = pClient.openTable(tableName)
        val kuduSession = pClient.newSession()
        kuduSession.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
        kuduSession.setIgnoreAllDuplicateRows(true)
        var insert: Operation = null
        try {
          it.foreach(event => {
            insert = table.newInsert()
            val row = insert.getRow
            row.addString("ticket",event.tickId)
            row.addString("per", event.period)
            val dateTime = convertTimestamp(event.day, event.time)
            row.addString("timestamp", dateTime)
            val ts = getTimstamp(dateTime)
            row.addLong("ts", ts)
            row.addLong("ts_received", System.currentTimeMillis)
            row.addString("last", event.last)
            row.addString("vol", event.volume)

            val r = kuduSession.apply(insert)
          })
        } catch {
          case e:Exception => e.printStackTrace()

        } finally {
          kuduSession.close()
        }
        // no pClient shutdown needed, handled by kudu context
      })
      parsed
    }

    parsedTickLines.count().print()

    sys.ShutdownHookThread {
      println("Gracefully stopping Spark Streaming Application")
      ssc.stop(true, true)
      println("Application stopped")
    }

    // Start the computation
    ssc.checkpoint("./checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }


  def convertTimestamp(dayStr: String, timeStr: String): String = {
    var dateStr = dayStr + timeStr
    var out = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    out.format((new SimpleDateFormat("yyyyMMddHHmmss").parse(dateStr)))
  }

  def getTimstamp(tsStr: String): Long = {
    var format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parsedTime = format.parse(tsStr)
    var ts = new java.sql.Timestamp(parsedTime.getTime());
    ts.getTime
  }

  def createTable(client: KuduClient, kuduMaster: String, tableName: String): Unit = {
    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val createOptions = new CreateTableOptions();
    createOptions.addHashPartitions(ImmutableList.of("ticket"), 4);
    if (kuduClient.tableExists(tableName)) {
      println("Deleting Table")
      kuduClient.deleteTable(tableName)
    }
    println("Creating Table")
    val table = kuduClient.createTable(tableName, schema, createOptions)
    println("Created Table")
    kuduClient.shutdown()
  }
}
