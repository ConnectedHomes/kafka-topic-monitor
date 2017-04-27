require 'kafka'

module HiveHome
  module KafkaTopicMonitor

    ##
    # A class that queries Kafka for topics and their end offsets.
    #
    # Author: Dmitry Andrianov
    #
    class TopicDataRetriever
      def initialize(seed_brokers:, client_id:)
        @seed_brokers = seed_brokers
        @client_id    = client_id
        reconnect
      end

      # Returns a hash of the structure: { topic => { partition => end-offset } }
      def get_topic_offsets
        cluster = @kafka.instance_variable_get('@cluster')
        result  = {}
        topics  = cluster.topics
        cluster.add_target_topics topics
        topics.each do |topic|
          partitions    = cluster.partitions_for(topic)
          ids           = partitions.collect(&:partition_id)
          offsets       = cluster.resolve_offsets(topic, ids, :latest)
          result[topic] = offsets
        end
        result
      rescue ::Kafka::ConnectionError, ::Kafka::LeaderNotAvailable => e
        puts e
        puts e.class
        puts e.backtrace
        puts "Topic data retriever: reconnecting to Kafka"
        reconnect
      rescue ::Kafka::NotLeaderForPartition
        puts "Topic data retriever: refreshing cluster metadata"
        cluster.refresh_metadata! # Refresh for next request
        raise
      end

      private

      def reconnect
        @kafka.close unless @kafka.nil? rescue nil
        @kafka = @kafka_client = ::Kafka.new(seed_brokers: @seed_brokers, client_id: @client_id)
      end
    end
  end
end
