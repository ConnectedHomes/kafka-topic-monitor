require 'kafka'
require 'ostruct'
require_relative 'kafka_decoder'
require_relative 'consumer_monitor'

module HiveHome
  module KafkaTopicMonitor
    
    ##
    # The main functionality of this application. This class repeatedly queries Kafka for consumer offset metrics
    # and publishes the result via the Graphite metric format.
    #
    # Author: Dmitry Andrianov, Talal Al-Tamimi
    # 
    class Reporter
      def initialize(sender, options)
        @sender  = sender
        @opts = options
      end

      def run
        # Ruby Kafka client is not thread safe.
        # https://github.com/zendesk/ruby-kafka#thread-safety says network communications are not synchronized
        # so we need two clients - one for getting consumer offsets and one for getting topic end offsets
        # Depending on reporting options combination, we could avoid creating one of them but it is not a big deal
        # as no connection is established at this moment.
        kafka1 = ::Kafka.new(seed_brokers: @opts.brokers, client_id: File.basename(__FILE__))
        kafka2 = ::Kafka.new(seed_brokers: @opts.brokers, client_id: File.basename(__FILE__))

        @data_retriever   = kafka1
        @consumer_monitor = ConsumerDataMonitor.new(kafka2)

        if @opts.report_consumer_offsets || @opts.report_consumer_lag
          @consumer_monitor.start
          # Let it collect some data
          sleep 5
        end

        while true
          begin
            report
          rescue => e
            puts "[#{Time.now}] Error in reporter main loop: #{e.class} - #{e.message}"
            puts e.backtrace
          end
          sleep @opts.interval
        end
      end

      def report
        time             = Time.new
        consumer_offsets = @consumer_monitor.get_consumer_offsets
        topic_offsets    = @data_retriever.last_offsets

        report_end_offsets(time, topic_offsets)                      if @opts.report_end_offsets
        report_consumer_offsets(time, consumer_offsets)              if @opts.report_consumer_offsets
        report_partition_lags(time, consumer_offsets, topic_offsets) if [:both, :partition].include? @opts.report_consumer_lag
        report_topic_lags(time, consumer_offsets, topic_offsets)     if [:both, :total]    .include? @opts.report_consumer_lag
      end

      private

      def report_end_offsets(time, topic_offsets)
        topic_offsets.each do |topic, partition_offsets|
          partition_offsets.each do |partition, end_offset|
            @sender.publish(time, ['topic', topic, 'partition', partition, 'end_offset'], end_offset)
          end
        end
      end

      def report_consumer_offsets(time, consumer_offsets)
        each_paritition(consumer_offsets) do |group, topic, partition, offset|
          @sender.publish(time, ['group', group, 'topic', topic, 'partition', partition, 'consumer_offset'], offset)
        end
      end

      def report_partition_lags(time, consumer_offsets, topic_offsets)
        each_paritition(consumer_offsets) do |group, topic, partition, offset|
          next if topic_offsets[topic].nil? || topic_offsets[topic][partition].nil?
          end_offset = topic_offsets[topic][partition]
          lag = end_offset - offset
          @sender.publish(time, ['group', group, 'topic', topic, 'partition', partition, 'lag'], lag)
        end
      end

      def report_topic_lags(time, consumer_offsets, topic_offsets)
        each_topic(consumer_offsets) do |group, topic, offset|
          next if topic_offsets[topic].nil?
          end_offset = topic_offsets[topic].values.inject(:+)
          lag = end_offset - offset
          @sender.publish(time, ['group', group, 'topic', topic, 'total', 'lag'], lag)
        end
      end

      # :call-seq:
      # each_paritition(hash) { |consumer_group, topic, partition, offset| ... }
      def each_paritition(consumer_offsets)
        consumer_offsets.each do |group, group_offsets|
          group_offsets.each do |topic, topic_offsets|
            topic_offsets.each do |partition, offset|
              yield(group, topic, partition, offset) if block_given?
            end
          end
        end
      end

      # :call-seq:
      # each_topic(hash) { |consumer_group, topic, offset| ... }
      def each_topic(consumer_offsets)
        consumer_offsets.each do |group, group_offsets|
          group_offsets.each do |topic, topic_offsets|
            offset = topic_offsets.values.inject(:+)
            yield(group, topic, offset) if block_given?
          end
        end
      end

    end
  end
end

module Kafka
  class Client

    # Fetches end offests for specified topics.
    #
    # @param topics [String, Array<String>] single topic name or array of topic names.
    #   nil means all topics will be fetched.
    #
    # @return [Hash] {
    #     topic_name [String] => {
    #       partition_id [Integer] => end_offset [Integer], ...
    #     }, ...
    #   }
    def last_offsets(topics = nil)
      topics   = [topics] if !topics.nil? && topics.is_a?(String)
      topics ||= self.topics
      @cluster.add_target_topics(topics)

      result = {}
      topics.each do |topic|
        partition_ids = @cluster.partitions_for(topic).collect(&:partition_id)
        begin
          result[topic] = @cluster.resolve_offsets(topic, partition_ids, :latest)
        rescue ProtocolError
          # Once we submit a patch for Cluster#resolve_offsets with
          # a similar rescue statement, this one can be removed.
          @cluster.mark_as_stale!
          raise
        end
      end
      result
    end

  end
end
