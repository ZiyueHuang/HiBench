/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.sparkbench.structuredstreaming.application

import com.intel.hibench.common.streaming.metrics.KafkaReporter
import com.intel.hibench.sparkbench.structuredstreaming.util.SparkBenchConfig
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

import java.util.Properties
import org.apache.kafka.clients.producer._

class StructuredIdentity() extends StructuredBenchBase {

  override def process(ds: DataFrame, config: SparkBenchConfig) = {

    // Get the singleton instance of SparkSession
    val spark = SparkSession.builder.appName("structured " + config.benchName).getOrCreate()
    import spark.implicits._

    val query = ds.as[(String, String)].groupByKey(x=>x._1)
      .mapGroupsWithState[String, String](GroupStateTimeout.NoTimeout) {

        case (key: String, values: Iterator[(String, String)], state: GroupState[String]) =>
          {
            state.update(key)
            key
          }
      }
      .writeStream
      .outputMode("update")
      .foreach(new ForeachWriter[String] {
        //var reporter: KafkaReporter = _

        val props = new Properties()
        props.put("bootstrap.servers", "10.1.2.91:9092,10.1.2.92:9093,10.1.2.93:9094")  
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        var producer: KafkaProducer[String, String] = _

        def open(partitionId: Long, version: Long): Boolean = {
          //val reportTopic = config.reporterTopic
          //val brokerList = config.brokerList
          //reporter = new KafkaReporter(reportTopic, brokerList) 
          producer = new KafkaProducer[String, String](props)
          true
        }

        def close(errorOrNull: Throwable): Unit = {producer.close()}

        def process(record: String): Unit = {
          val inTime = record.toLong
          val outTime = System.currentTimeMillis()
          //reporter.report(inTime, outTime)
          producer.send(new ProducerRecord(config.reporterTopic, "", inTime+":"+outTime))
        }
      })
      //.format("console")
      .start()

    query.awaitTermination()
  }
}
