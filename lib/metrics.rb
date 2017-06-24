module HiveHome
  module KafkaTopicMonitor

    ## 
    # A simple storage for internal metrics
    #
    # Author: Dmitry Andrianov
    #
    class Metrics

      def initialize()
        @registry  = {}
        @mutex     = Mutex.new
      end

      def increment(*name)
        @mutex.synchronize do
          @registry[*name] = (@registry[*name] || 0) + 1
        end
      end

      def get_metrics
        # Return a deep copy with a snapshot of data
        data = @mutex.synchronize do
          Marshal.dump(@registry)
        end
        Marshal.load(data)
      end

    end
  end
end
