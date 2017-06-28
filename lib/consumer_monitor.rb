require 'kafka'
require_relative 'kafka_decoder'

module HiveHome
  module KafkaTopicMonitor

    ## 
    # A background job that repeatedly queries Kafka for consumer offset data.
    #
    # Author: Dmitry Andrianov
    #
    class ConsumerDataMonitor
      def initialize(kafka)
        @kafka    = kafka
        @data     = {}
        @mutex    = Mutex.new
      end

      def start
        Thread.new do
          while true
            begin
              run
            rescue => e
              puts "[#{Time.now}] Error in consumer data monitor: #{e.class} - #{e.message}"
              puts e.backtrace
            end
            sleep 60
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

          if message.value.nil? # nil message body means topic is marked for deletion
            delete_topic(key.topic)
          elsif key.is_a? GroupTopicPartition
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

      def delete_topic(topic)
        @mutex.synchronize do
          @data.each_key { |group| @data[group].delete(topic) }
        end
      end

    end
  end
end
