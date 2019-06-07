# kafka-topic-monitor
Kafka topic monitoring tool

The tool allows collecting some topic metrics from Kafka and reporing them to Graphite or a similar system using Carbon protocol.
The main purpose was to monitor consumer lag.

Note that this tool was designed to be used with Kafka 0.9 new consumers only - the consumer offsets are read from Kafka's `__consumer_offset` topic.

## Installation
You need as least Ruby 2.1 because it is required by `ruby-kafka`. Then install the `ruby-kafka` itself, either via Bundler (recommended):

```sh
bundle install
```
or manually:
```sh
sudo gem install ruby-kafka
```

## Running
At the very minimum you need to supply address of one of your Kafka brokers and server where metrics should be reported
```sh
kafka-topic-monitor --broker KAFKASERVER --metrics-server GRAPHITESERVER --consumer-lag=total
```
This way the tool will be generating these metrics every minute: `group.<CONSUMER_GROUP>.topic.<TOPIC_NAME>.lag` (one value for each consumer group, each topic).

You can specify prefix for metric names with `--metrics-base` option. In that case the prefix will be appended in front of every metric reported.

With `--interval` option you can specify how often data needs to be reported. Interval is specified in seconds and 60 is the default.

## Additional metrics

### --consumer-lag=total
Reports total consumer lag for a group/topic:
```
group.<CONSUMER_GROUP>.topic.<TOPIC_NAME>.lag
```
### --consumer-lag=partition
Reports detailed consumer lag for each group/topic/partition:
```
group.<CONSUMER_GROUP>.topic.<TOPIC_NAME>.partition.<N>.lag
```
### --consumer-lag=both
Reports both total lag for group/topic and detailed for group/topic/partition

### --end-offset
Reports topic end offset each group/topic/partition:
```
group.<CONSUMER_GROUP>.topic.<TOPIC_NAME>.partition.<N>.end_offset
```

### --consumer-offset
Reports topic end offset each group/topic/partition:
```
group.<CONSUMER_GROUP>.topic.<TOPIC_NAME>.partition.<N>.consumer_offset
```

### --internal-metrics
Reports monitor's internal stats that can be useful to spot problems like it regularly losing track of consumer offsets because of exceptions etc:
```
internal.Reporter.report.count
internal.Reporter.report.avg
internal.Reporter.exceptions

internal.GraphiteSender.publish
internal.GraphiteSender.exceptions

internal.ConsumerDataMonitor.messages
internal.ConsumerDataMonitor.offset.update
internal.ConsumerDataMonitor.exceptions
```

### How test locally

Assuming you have two Kafka brokers running locally on ports 9092 and 9093, and assuming your metrics server is running locally on port 2003, then the following command, executed from the repository directory, will run the topic monitor:
```sh
bundle exec bin/kafka-topic-monitor \
--broker localhost:9092 \
--broker localhost:9093 \
--consumer-lag both \
--metrics-server localhost:2003 \
--metrics-base 'dev.kafka.monitor' \
--interval 3
```

