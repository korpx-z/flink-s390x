# Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)


### Features

* A streaming-first runtime that supports both batch processing and data streaming programs

* Elegant and fluent APIs in Java and Scala

* A runtime that supports very high throughput and low event latency at the same time

* Support for *event time* and *out-of-order* processing in the DataStream API, based on the *Dataflow Model*

* Flexible windowing (time, count, sessions, custom triggers) across different time semantics (event time, processing time)

* Fault-tolerance with *exactly-once* processing guarantees

* Natural back-pressure in streaming programs

* Libraries for Graph processing (batch), Machine Learning (batch), and Complex Event Processing (streaming)

* Built-in support for iterative programs (BSP) in the DataSet (batch) API

* Custom memory management for efficient and robust switching between in-memory and out-of-core data processing algorithms

* Compatibility layers for Apache Hadoop MapReduce

* Integration with YARN, HDFS, HBase, and other components of the Apache Hadoop ecosystem


### Streaming Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.socketTextStream(host, port, '\n')

val windowCounts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .keyBy("word")
  .timeWindow(Time.seconds(5))
  .sum("count")

windowCounts.print()
```

### Batch Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.readTextFile(path)

val counts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .groupBy("word")
  .sum("count")

counts.writeAsCsv(outputPath)
```


## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org).



## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.
