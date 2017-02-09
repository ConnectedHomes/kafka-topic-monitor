module HiveHome

  ##
  # Publishes metrics to a server using the Graphite data format.
  # A Graphite metric is a single line of plain text of the form:
  #   hierarchical.dot.delimited.metric.name <value> <timestamp>
  #
  # Author: Dmitry Andrianov
  #
  class GraphiteSender
    def initialize(server, metric_base)
      @host, @port = server.split(':', 2)
      @port ||= 2003
      @metric_base = metric_base
    end

    #====Parameters:
    # time::        Integer - number of seconds from the Epoch (1-Jan-1970 0:00 GMT).
    # metric_name:: Array<String> - components of metric name. Will be combined into dot-delimited string.
    # value::       Integer - metric reading.
    def publish(time, metric_name, value)
      metric = metric_name.collect { |p| escape(p) }.join('.')
      metric = @metric_base + '.' + metric unless @metric_base.nil?
      connect
      begin
        @socket.puts "#{metric} #{value} #{time.to_i}"
      rescue => e
        puts "Error writing to the socket: #{e}"
        close
      end
    end

    private

    def escape(param)
      param.to_s.gsub(/\./, '\\.') unless param.nil?
    end

    def connect
      @socket = TCPSocket.new(@host, @port) if @socket.nil?
    end

    def close
      return if @socket.nil?
      begin
        @socket.close
      rescue => e
        puts "Error closing the socket: #{e}"
      end
      @socket = nil
    end
  end
end
