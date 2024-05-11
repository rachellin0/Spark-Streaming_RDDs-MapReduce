# Spark_Streaming_RDDs_MapReduce
## Spark Streaming:
Spark Streaming is a component of the Apache Spark ecosystem that enables the processing of real-time data streams. It allows developers to write streaming applications that consume data from various sources (such as Kafka, Flume, Kinesis, or TCP sockets) and process the data using Spark's high-level functions. Spark Streaming receives live input data and divides it into batches, which are then processed by the Spark engine to generate the final stream of results.
## RDDs (Resilient Distributed Datasets):
RDDs are the fundamental data structure in Apache Spark. They are immutable, fault-tolerant, and distributed collections of objects that can be processed in parallel across a cluster. RDDs are created from various data sources (e.g., files, databases, or existing RDDs) and can be transformed using parallel operations like map, filter, and reduce. Spark Streaming internally uses RDDs to represent the streaming data batches and applies transformations on them.
## MapReduce:
MapReduce is a programming model and an associated implementation for processing and generating large datasets with a parallel, distributed algorithm on a cluster. It consists of two main phases:

### Map Phase: 
The input data is divided into independent chunks that are processed by the map tasks in parallel. Each map task takes one chunk of the input data and produces a set of intermediate key-value pairs.
### Reduce Phase: 
The output from the map tasks is shuffled and sorted by the framework, then passed to the reduce tasks. Each reduce task processes the values associated with a particular key and produces the final output.
# real-time data stream displays 
![Screen Shot 2024-05-10 at 10 31 05 PM](https://github.com/rachellin0/Spark_Streaming_RDDs_MapReduce/assets/91289121/00a8cfc4-8430-4237-b39b-e9a6d31c2c38)
