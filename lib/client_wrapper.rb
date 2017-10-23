require 'kafka'

module HiveHome
  module KafkaTopicMonitor

    ##
    # Wrapper around a Kafka client that re-initilizes the client after a connection error.
    # Like the object it wraps, this class is not thread-safe.
    #
    # Author: Talal Al-Tamimi
    #
    class KafkaClientWrapper

      RESET_ON_CONNECTION_ERROR = true

      #=====Parameters:
      # seed_brokers:: Array<String> - array of brokers, each of the form "hostname:port"
      # client_id::    String - identifier for this application
      def initialize(seed_brokers:, client_id: 'kafka-topic-monitor')
          @seed_brokers = seed_brokers
          @client_id    = client_id
          init_client
      end

      def method_missing(sym, *args, &block)
        @client.send(sym, *args, &block)
      rescue Kafka::ConnectionError => e
        if RESET_ON_CONNECTION_ERROR
          puts "[#{Time.now}] Connection error #{e.class}, will try to reconnect - #{e.message}"
          close_client
          init_client
        end
        raise e
      end

      private

      def init_client
        # This doesn't actually establish any connections to brokers. Connections are made the first
        # time a request-sending method is called.
        @client = ::Kafka.new(seed_brokers: @seed_brokers, client_id: @client_id)
      end

      def close_client
        @client.close unless @client.nil?
        @client = nil
      rescue
        puts "[#{Time.now}] Couldn't close old connection; ignoring. #{e.class} - #{e.message}"
      end

    end
  end
end
