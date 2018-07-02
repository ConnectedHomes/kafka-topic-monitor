require_relative 'metrics'

module HiveHome

  ##
  # Publishes metrics to a server using the Graphite data format.
  # A Graphite metric is a single line of plain text of the form:
  #   hierarchical.dot.delimited.metric.name <value> <timestamp>
  #
  # Author: Dmitry Andrianov
  #
  class GraphiteSender

    attr_reader :metrics

    def initialize(server, metric_base, logger)
      @host, @port = server.split(':', 2)
      @port ||= 2003
      @metric_base = metric_base
      @metrics = KafkaTopicMonitor::Metrics.new
      @logger = logger
    end

    # @param time [Integer] number of seconds from the Epoch (1-Jan-1970 0:00 GMT).
    # @param metric_name [Array<String>] components of metric name. Will be combined into dot-delimited string.
    # @param value [Integer] metric reading.
    def publish(time, metric_name, value)
      @metrics.increment(['publish', 'count'])
      metric = metric_name.collect { |p| escape(p) }.join('.')
      metric = @metric_base + '.' + metric unless @metric_base.nil?
      connect
      begin
        @socket.puts "#{metric} #{value} #{time.to_i}"
      rescue => e
        @logger.error("Error writing to metrics socket: #{e.class} - #{e.message}")
        @metrics.increment(['exceptions'])
        close
      end
    end

    private

    def escape(param)
      param.to_s.gsub(/\./, '_') unless param.nil?
    end

    def connect
      @socket = TCPSocket.new(@host, @port) if @socket.nil?
    end

    def close
      return if @socket.nil?
      begin
        @socket.close
      rescue => e
        @logger.error("Error closing metrics socket: #{e.class} - #{e.message}")
        @metrics.increment(['exceptions'])
      end
      @socket = nil
    end
  end
end
