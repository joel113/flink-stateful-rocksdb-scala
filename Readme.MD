# Apache Flink Data Stream API Example

Example which uses the Flink Data Stream API for a stateful event stream processing.

This example listens to an Apache Kafka topic, processes the data from the topic and increments a stateful
variable based on the passed data.

The example is based on the Apache Flink Data Stream API, a keyed data stream implementation and keyed state.

Please consider reading following resources for more information.

## Apache Flink Documentation

### Flink Data Stream API Programming Guide

https://ci.apache.org/projects/flink/flink-docs-stable/dev/datastream_api.html

### Flink Working with State Documentation

https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/state.html

### Flink State Backends Documentation

https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/state_backends.html

### Flink Data Stream API Fraud Detection Example

https://ci.apache.org/projects/flink/flink-docs-release-1.12/try-flink/datastream_api.html

### Flink Rocks DB Stateful

https://medium.com/swlh/run-a-stateful-streaming-service-with-apache-flink-and-rocksdb-749fbc24d78f

### Flink Filesystem Configuration

https://ci.apache.org/projects/flink/flink-docs-master/deployment/filesystems/#hadoop-file-system-hdfs-and-its-other-implementations

## Futher Resources

https://medium.com/big-data-processing/flink-map-comap-richmap-and-richcomap-functions-52a204745a7
