package com.cloudera.examples

import java.text.SimpleDateFormat
import java.util.Properties
import java.util.concurrent.ExecutionException


import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

import scala.util.Properties
;

/**
  * Created by bosshart on 5/7/16.
  */
object KafkaPopulator {

    def main(args: Array[String]) {
      if (args.length < 1) {
        System.err.println("Usage: " + this.getClass.getSimpleName + " <inputDirectory> <zklist>")
        ///Users/bosshart/Downloads/dlp-test.avro ip-10-1-1-228.us-west-2.compute.internal:2181/solr 2
        System.exit(1)
      }
      val inputPath = args(0) //input directory for DLP avro data
      val brokerList = args(1)
      val props = new Properties()
      val topic = "marketdata"
      props.put("bootstrap.servers", brokerList);
      props.put("client.id", "DemoMarketProducer");
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

      val sql = sparkContext("Data Writer Example", true)
      val files = sql.sparkContext.wholeTextFiles(inputPath)


      files.map(rawFiles => {
        val companyName = rawFiles._1.split("[._]")(1)
        (companyName, rawFiles._2.split("\\r?\\n"))
      }).foreachPartition(files => {
        val producer = new KafkaProducer[String, String](props)
        files.foreach(file => {
          val key = file._1
          file._2.drop(1).foreach(line => {
            producer.send(new ProducerRecord[String, String](topic, key, line)).get();
          })

        })

      })
      /**files.foreachPartition(records=> {
        * records.foreach(record=> {

        * })

        * })
        * })**/

    }

    def toSolrDate(dateStr: String): String = {
      val out = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      out.format((new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").parse(dateStr)))
    }

    private def sparkContext(appName: String, isLocal: Boolean): SQLContext = {
      val sparkConf = new SparkConf().setAppName(appName)
      if (isLocal) {
        sparkConf.setMaster("local")
      }
      val sc = new SparkContext(sparkConf)
      new SQLContext(sc)
    }
  }
