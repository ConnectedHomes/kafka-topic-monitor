module HiveHome
  module KafkaTopicMonitor

    ## 
    # A simple storage for internal metrics
    #
    # Author: Dmitry Andrianov
    #
    class Metrics

      def initialize()
        @counters = Hash.new(0)
        @timers   = Hash.new { |hash, key| hash[key] = Timer.new(key) }
        @mutex    = Mutex.new
      end

      # Increment a counter metric
      def increment(name)
        name = name.split('.') if name.is_a? String
        @mutex.synchronize do
          @counters[name] +=1
        end
      end

      def timer(name)
        name = name.split('.') if name.is_a? String
        @mutex.synchronize do
          # Will auto-create, see how hash is constructed
          @timers[name]
        end
      end

      def get_metrics
        result = {}
        @mutex.synchronize do
          @counters.each { |name, value| result[name] = value }
          @timers.each { |name, timer|
            (count, min, max, avg) = timer.restart
            result[[*name, 'count']] = count if count
            result[[*name, 'min']] = min if min
            result[[*name, 'max']] = max if max
            result[[*name, 'avg']] = avg if avg
          }
        end
        result
      end

    end

    class Timer

      attr_reader :name

      def initialize(name)
        @mutex = Mutex.new
        @name = name
        @total_count = 0
        reset
      end

      def start
        TimerContext.new(self)
      end

      def stop(timer_context)
        @mutex.synchronize do
          duration = timer_context.duration

          @duration += duration
          @total_count += 1
          @count += 1

          @min = [@min, duration].compact.min
          @max = [@max, duration].compact.max
          @avg = @duration / @count
        end
      end

      def restart
        @mutex.synchronize do
          result = [@total_count, @min, @max, @avg]
          reset
          result
        end
      end

      private

      def reset
        @duration = 0
        @count = 0
        @min = nil
        @max = nil
        @avg = nil
      end
    end

    class TimerContext

      def initialize(timer)
        @timer = timer
        @start = Time.now
      end

      def duration
        ((@end - @start) * 1000).to_i
      end

      def stop
        @end = Time.now
        @timer.stop(self)
      end
    end

  end
end
