require 'kafka'
require_relative 'kafka_decoder'
require_relative 'metrics'

module HiveHome
  module KafkaTopicMonitor

    ## 
    # A background job that repeatedly queries Kafka for consumer offset data.
    #
    # Author: Dmitry Andrianov
    #
    class ConsumerDataMonitor

      def log
        KafkaTopicMonitor.logger
      end

      attr_reader :metrics

      # @param kafka [Kafka::Client] initialization signifies a transfer of ownership. This means the lifecycle of the
      #   kafka client, including its proper termination, is now the responsibility of ConsumerDataMonitor.
      def initialize(kafka)
        @kafka    = kafka
        @data     = {}
        @mutex    = Mutex.new
        @metrics  = Metrics.new
      end

      def start
        Thread.new do
          while true
            begin
              run
            rescue Kafka::UnknownTopicOrPartition => e
              # PT1-2605: temporary patch until issue is fixed in ruby-kafka library
              # https://github.com/zendesk/ruby-kafka/issues/610
              # https://github.com/zendesk/ruby-kafka/pull/609
              log.error("Error in consumer data monitor: __consumer_offsets topic not found on broker. Was it reassigned?")
              log.info("Marking cluster as stale")
              @kafka.instance_variable_get(:@cluster).mark_as_stale!
            rescue => e
              log.error("Error in consumer data monitor: #{e.class} - #{e.message}")
              log.error(e.backtrace.join("\n"))
              @metrics.increment(['exceptions'])
            end
            sleep 15
          end
        end
      end

      # The most recently queried consumer offset data.
      # Returns a hash of the structure:
      #   { consumer-group => { topic  => { partition => consumer-offset } } }
      def get_consumer_offsets
        # Return a deep copy with a snapshot of data
        data = @mutex.synchronize do
          Marshal.dump(@data)
        end
        Marshal.load(data)
      end

      private

      def run
        @kafka.each_message(topic: '__consumer_offsets', start_from_beginning: false, max_wait_time: 0) do |message|

          @metrics.increment(['messages'])

          key = Decoder.decode_key(message.key)

          if key.is_a? GroupTopicPartition
            
            if message.value.nil? # nil message body means topic is marked for deletion
              @metrics.increment(['topic', 'delete'])
              delete_topic(key.topic)
            else
              # Consumer offset
              @metrics.increment(['offset', 'update'])

              offset = Decoder.decode_offset(message.value)

              register_consumer_offset(key.group, key.topic, key.partition, offset.offset)
            end
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
          @data.each_key do |group|
            unless @data[group].nil?
              @data[group].delete(topic)
              @data.delete(group) if @data[group].empty?
            end
          end
        end
      end

    end
  end
end
