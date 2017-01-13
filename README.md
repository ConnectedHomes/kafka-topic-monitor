# kafka-topic-monitor
Kafka topic monitoring tool

The tool allows collecting some topic metrics from Kafka and reporing them to Graphite or a similar system using Carbon protocol.
The main purpose was to monitor consumer lag.

## Installation
You need as least Ruby 2.1 because it is required by ruby-kafka. Then install the ruby-kafka itself:
```
sudo gem install ruby-kafka
```

## Running
At the very minimum you need to supply address of one of your Kafka brokers and server where metrics should be reported
```
kafka-topic-monitor --broker KAFKASERVER --metrics-server GRAPHITESERVER --consumer-lag=total
```
This way the tool will be generating these metrics every minute: "group.CONSUMER_GROUP.topic.TOPIC_NAME.lag" (one value for each consumer group, each topic).

You can specify prefix for metric names with --metrics-base option. In that case the prefix will be appended in front of every metric reported.

With --interval option you can specify how often data needs to be reported. Interval is specified in seconds and 60 is the default.

## Additional metrics

### --consumer-lag=total
Reports total consumer lag for a group/topic:
```
group.CONSUMER_GROUP.topic.TOPIC_NAME.lag
```
### --consumer-lag=partition
Reports detailed consumer lag for each group/topic/partition:
```
group.CONSUMER_GROUP.topic.TOPIC_NAME.partition.N.lag
```
### --consumer-lag=both
Reports metrics for both --consumer-lag=total and --consumer-lag=partition

### --end-offset
Reports topic end offset each group/topic/partition:
```
group.CONSUMER_GROUP.topic.TOPIC_NAME.partition.N.end_offset
```
### --consumer-offset
Reports topic end offset each group/topic/partition:
```
group.CONSUMER_GROUP.topic.TOPIC_NAME.partition.N.consumer_offset
```
