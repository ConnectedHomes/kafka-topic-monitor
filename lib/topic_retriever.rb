require 'kafka'

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
