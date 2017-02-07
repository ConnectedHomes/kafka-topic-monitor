require 'kafka'
require_relative 'kafka_decoder'

module HiveHome
  module Kafka

    ## 
    # A background job that repeatedly queries Kafka for consumer offset data.
    #
    # Author: Dmitry Andrianov
    #
    class ConsumerDataMonitor
      def initialize(kafka)
        @kafka = kafka
        @data  = {}
        @mutex = Mutex.new
      end

      def start
        Thread.new do
          begin
            run
          rescue => e
            puts "Error in consumer data monitor: #{e}"
            puts "#{e.backtrace}"
          end
        end
      end

      # The most recently queried consumer offset data.
      # Returns a hash of the structure:
      #   { consumer-group => { topic  => { partition => consumer-offset } } }
      def get_consumer_offsets
        # Return a deep copy with a snapshot of data
        @mutex.synchronize do
          Marshal.load(Marshal.dump(@data))
        end
      end

      private

      def run
        @kafka.each_message(topic: '__consumer_offsets', start_from_beginning: false, max_wait_time: 0) do |message|
          key = Decoder.decode_key(message.key)
          if key.is_a? GroupTopicPartition
            # Consumer offset
            offset = Decoder.decode_offset(message.value)
            register_consumer_offset(key.group, key.topic, key.partition, offset.offset)
          end
        end
      end

      def register_consumer_offset(group, topic, partition, offset)
        @mutex.synchronize do

          @data[group] ||= {}
          group_data = @data[group]

          group_data[topic] ||= {}
          topic_data = group_data[topic]

          topic_data[partition] = offset
        end
      end

    end
  end
end
