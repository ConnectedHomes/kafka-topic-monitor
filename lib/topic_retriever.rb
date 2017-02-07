require 'kafka'

module HiveHome
  module Kafka

    ##
    # A class that queries Kafka for topics and their end offsets.
    #
    # Author: Dmitry Andrianov
    #
    class TopicDataRetriever
      def initialize(kafka)
        @cluster = kafka.instance_variable_get("@cluster")
      end

      # Returns a hash of the structure:
      #   { topic => { partition => end-offset } }
      def get_topic_offsets
        result  = {}
        topics  = @cluster.topics
        @cluster.add_target_topics topics
        topics.each do |topic|
          partitions    = @cluster.partitions_for(topic)
          ids           = partitions.collect(&:partition_id)
          offsets       = @cluster.resolve_offsets(topic, ids, :latest)
          result[topic] = offsets
        end
        result
      end
    end

  end
end
