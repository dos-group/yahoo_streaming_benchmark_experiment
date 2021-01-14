# yahoo-benchmark-experiment

This repository consists of a Kafka event generator and a Flink processor. The code is adapted from the [Yahoo streaming benchmark](https://github.com/yahoo/streaming-benchmarks) (blog post [here](https://yahooeng.tumblr.com/post/135321837876/benchmarking-streaming-computation-engines-at)).

In this advertising use case, ad events are generated by a Kafka producer in a JSON format. The events are parsed, filtered for the ad "view" events, unneeded fields are removed, and new fields are added by joining the event with campaign data stored in Redis. Views are then aggregated by campaign and by time window and stored back into Redis, along with a timestamp to indicate when they are updated.

## Requirements
[Maven](https://maven.apache.org) and [Leiningen](https://leiningen.org) must be installed.

Tests require Zookeeper, Kafka, Redis, HDFS, and Flink (1.11.2).

## Configuration

Kafka and Redis configurations need to be specified for both the generator and Flink processor.

Generator: setup/resources/benchmarkConf.yaml

Processor: flink/src/main/resources/advertising.properties

## Event generator

Leiningen can be used to run various commands within the setup directory.

Before events can be generated, campaign ids need to be written to Redis:
```bash
lein run -n
```

After campaign ids are written, a configurable number of events can be emitted each second from a Kafka producer with:
```bash
lein run -r -t [number of events to emit per second]
```
To execute the generator in the background use:
```bash
lein rein -r -t [number of events to emit per second] > /dev/null 2 > & 1 &
```
The generator will continue to produce events until the process is terminated.

## Flink processor

To create a jar for the Flink processor, run ``mvn clean && mvn package`` from the root directory.

The newly created "flink-0.0.1-SNAPSHOT.jar" can then be submitted to the Flink cluster via the Flink dashboard or by running:
```bash
flink run flink/target/flink-0.0.1-SNAPSHOT.jar [checkpoint interval (ms)]
```
The processor expects the checkpoint interval in milliseconds as an argument.

Note: The number of task managers must be greater or equal to the number of Kafka partitions specified in the configuration file.

## Results

Results are written directly to Redis.
Running ``lein run -g`` in the setup directory will create two text files, seen.txt and updated.txt. seen.txt contains the counts of events for different campaigns and time windows. updated.txt is the latency in ms from when the last event was emitted to Kafka for that particular campaign window and when it was written into Redis.
