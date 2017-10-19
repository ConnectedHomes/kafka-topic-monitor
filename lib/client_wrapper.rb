require 'kafka'

module HiveHome
  module KafkaTopicMonitor

    ##
    # Wrapper around a Kafka client that re-initilizes the client after a connection error.
    # Like the object it wraps, this class is not thread-safe.
    #
    class KafkaClientWrapper

      #=====Parameters:
      # seed_brokers:: Array<String> - list of brokers of the from "hostname:port"
      # client_id::    String - identifier for this application
      def initialize(seed_brokers:, client_id: 'kafka-topic-monitor')
          @seed_brokers = seed_brokers
          @client_id    = client_id
          @client       = ::Kafka.new(seed_brokers: @seed_brokers, client_id: @client_id)
      end

      def method_missing(sym, *args, &block)
        @client.send(sym, *args, &block)
      rescue Kafka::ConnectionError => e
        puts "[#{Time.now}] Connection error: #{e.class} - #{e.message}"
        puts e.backtrace
        puts "[#{Time.now}] Will try to reconnect"

        @client.close unless @client.nil?
        @client = ::Kafka.new(seed_brokers: @seed_brokers, client_id: @client_id)
        nil
      end
    end

  end
end
