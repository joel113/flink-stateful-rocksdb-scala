package com.joel.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties
import scala.util.Random

object Main {

  def main(args: Array[String]) : Unit = {

    val streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumer = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), getKafkaConsumerProperties("test123"))

    val srcStream = streamExecutionEnvironment.addSource(kafkaConsumer)

    val random = Random

    val outStream = srcStream
      .map {
          new MapFunction[String, (String, String, Int)] {
            override def map(row: String): (String, String, Int) = {
              val columns = row.split(" ")
              (columns(0), columns(1), columns(2).toInt) // to int conversion might fail
            }
          }
      }
      // partitions the stream into disjoint partitions (cmp. https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/)
      .keyBy {
        new KeySelector[(String, String, Int), String] {
          override def getKey(in: (String, String, Int)): String = {
            in._1
          }
        }
      }
//      .flatMap {
//        // rich flatmap with access to the runtime context
//        new RichFlatMapFunction[KeyValue[String, String], String] {
//
//          // keyed value state (cmp. https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/state.html#using-keyed-state)
//          var previousInt: Option[ValueState[Integer]] = None
//          var nextInt: Option[ValueState[Integer]] = None
//
//          override def open(configuration: Configuration): Unit = {
//            super.open(configuration)
//            // the value state descriptor creates partitioned value state
//            previousInt = Some(getRuntimeContext.getState(new ValueStateDescriptor[Integer]("previousInt", classOf[Integer])))
//            nextInt = Some(getRuntimeContext.getState(new ValueStateDescriptor[Integer]("nextInt", classOf[Integer])))
//          }
//
//          override def flatMap(keyValue: KeyValue[String, String], collector: Collector[String]): Unit = {
//            try {
//              val oldInt = Integer.parseInt(keyValue.getValue)
//              val newInt = {
//                if(previousInt.isEmpty) {
//                  val newInt = oldInt
//                  collector.collect("OLD INT: " + oldInt.toString)
//                  newInt
//                }
//                else {
//                  val newInt = oldInt - previousInt.get.value()
//                  collector.collect("NEW INT: " + newInt.toString)
//                  newInt
//                }
//              }
//              nextInt.get.update(newInt)
//              previousInt.get.update(oldInt)
//            }
//            catch {
//              case e: Exception => e.printStackTrace()
//            }
//          }
//        }

      }

  def getKafkaConsumerProperties(groupId: String) : Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", groupId)
    properties
  }

}
