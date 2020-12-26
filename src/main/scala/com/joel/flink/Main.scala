package com.joel.flink

import com.joel.flink.model.Vehicle
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import java.util.Properties

object Main {

  def getKafkaConsumerProperties(groupId: String): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", groupId)
    properties
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumer = new FlinkKafkaConsumer[String]("vehicles", new SimpleStringSchema(), getKafkaConsumerProperties("flink"))

    val srcStream = env.addSource(kafkaConsumer)

    val outStream = srcStream
      .map {
        new MapFunction[String, Vehicle] {
          override def map(row: String): Vehicle = {
            val columns = row.split(" ")
            new Vehicle(columns(0), columns(1), columns(2).toInt) // to int conversion might fail
          }
        }
      }
      // partitions the stream into disjoint partitions (cmp. https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/)
      .keyBy {
        new KeySelector[Vehicle, String] {
          override def getKey(in: Vehicle): String = {
            in.vin
          }
        }
      }
      .flatMap {
        new RichFlatMapFunction[Vehicle, Vehicle] {

          // keyed value state (cmp. https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/state.html#using-keyed-state)
          @transient var mileageSum: ValueState[Integer] = _

          /*
          Flink rich functions provide additional methods (open, close, getRuntimeContext, setRuntimeContext). Open is important as it
          is makes a function stateful by initializing the state. It is only called once.
           */
          override def open(configuration: Configuration): Unit = {
            super.open(configuration)
            // the value state descriptor creates partitioned value state
            mileageSum = getRuntimeContext.getState(new ValueStateDescriptor[Integer]("previousInt", classOf[Integer]))
          }

          override def flatMap(in: Vehicle, collector: Collector[Vehicle]): Unit = {
            if(mileageSum.value() == null) {
              mileageSum.update(in.mileage)
              println(s"Initialized the mileage sum of vehicle ${in.vin} with ${mileageSum.value()}.")
            }
            else {
              mileageSum.update(mileageSum.value + in.mileage)
              println(s"Updated the mileage sum of vehicle ${in.vin} to ${mileageSum.value()}.")
            }
          }

        }
      }

    // convenient data sink which writes the outstream to standard out
    outStream.print()

    env.execute()

  }
}
