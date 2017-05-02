require 'kafka'

# Open up 3rd-party class and add convenience methods
module Kafka
  class Client
    # Returns a hash of the structure: { topic:String => { partition:Int => end_offset:Int } }
    def topic_offsets
      topics.map { |topic| [topic, last_offsets_for(topic)] } .to_h
    end

    # Returns a hash of the structure: { partition:Int => end_offset:Int }
    def last_offsets_for(topic)
      @cluster.refresh_metadata_if_necessary!
      partition_ids = @cluster.partitions_for(topic).collect(&:partition_id)
      @cluster.resolve_offsets(topic, partition_ids, :latest)
    end
  end
end
