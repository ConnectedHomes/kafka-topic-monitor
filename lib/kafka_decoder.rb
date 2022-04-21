require 'kafka'

module HiveHome
  module KafkaTopicMonitor
    
    ## 
    # A collection of class (static) methods for decoding the binary data read from the
    # __consumer_offsets topic.
    # Logic is based on
    # https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala
    # 
    # Author: Dmitry Andrianov
    # 
    class Decoder
      class << self
        # See https://github.com/apache/kafka/blob/839b886f9b732b151e1faeace7303c80641c08c4/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1133-L1144
        def decode_key(value)
          decoder = ::Kafka::Protocol::Decoder.from_string(value)
          schema = decoder.int16

          if schema == 0 || schema == 1
            # Offset
            # group (string), topic (string), partition (int32)
            GroupTopicPartition.new decoder.string, decoder.string, decoder.int32
          elsif schema == 2
            # Group metadata
            Group.new decoder.string
          else
            # Unknown
            nil
          end
        end

        # See https://github.com/apache/kafka/blob/252e09501d08a36fa85e146239e114a71a944331/core/src/main/resources/common/message/OffsetCommitValue.json
        def decode_offset(value)
          return nil if value.nil?

          decoder = ::Kafka::Protocol::Decoder.from_string(value)
          schema = decoder.int16
          if (0..3).include? schema
            # Structure for schema=0:
            #   offset (int64), metadata (string), timestamp (int64)
            # Structure for schema=1:
            #   offset (int64), metadata (string), commit_timestamp (int64), expire_timestamp (int64)
            # Structure for schema=2:
            #   offset (int64), metadata (string), commit_timestamp (int64)
            # Structure for schema=3:
            #   offset (int64), leaderEpoch (int32), metadata (string), commit_timestamp (int64)
            #
            # We do not care about metadata or timestamp so lets handle both schemas the same way
            Offset.new decoder.int64
          else
            # Unknown
            raise "Unknown offset value schema #{schema}"
          end
        end

        # See https://github.com/apache/kafka/blob/839b886f9b732b151e1faeace7303c80641c08c4/core/src/main/resources/common/message/GroupMetadataValue.json
        def decode_metadata(value)
          return nil if value.nil?

          decoder = Kafka::Protocol::Decoder.from_string(value)
          schema = decoder.int16

          if schema == 0 || schema == 1
            # Structure is:
            #   protocol_type (string), generation (int32), protocol (string), leader (nullable string), members[]
            # and each member is for schema=0
            #   member_id (string), client_id (string), client_host (string), session_timeout (int32), subscription (bytes), assignment (bytes)
            # and for schema=1
            #   member_id (string), client_id (string), client_host (string), rebalance_timeout (int32), session_timeout (int32), subscription (bytes), assignment (bytes)
            GroupMetadata.new decoder.string, decoder.int32, decoder.string, decoder.string, decoder.array {
              GroupMember.new(
                member_id: decoder.string,
                client_id: decoder.string,
                client_host: decoder.string,
                rebalance_timeout: (schema == 1) ? decoder.int32 : nil,
                session_timeout: decoder.int32,
                subscription: decoder.bytes,
                assignment: decoder.bytes
              )
            }
          elsif schema == 2 || schema == 3
            # Structure is:
            #   protocol_type (string), generation (int32), protocol (string), leader (nullable string), current_state_timestamp (int64), members[]
            # and each member is for schema = 2:
            #   member_id (string), client_id (string), client_host (string), rebalance_timeout (int32), session_timeout (int32), subscription (bytes), assignment (bytes)
            # and for schema = 3:
            #   member_id (string), group_instance_id (nullable string), client_id (string), client_host (string), rebalance_timeout (int32), session_timeout (int32), subscription (bytes), assignment (bytes)
            GroupMetadata.new decoder.string, decoder.int32, decoder.string, decoder.string, decoder.int64, decoder.array {
              GroupMember.new(
                member_id: decoder.string,
                group_instance_id: schema == 3 ? decoder.string : nil,
                client_id: decoder.string,
                client_host: decoder.string,
                rebalance_timeout: decoder.int32,
                session_timeout: decoder.int32,
                subscription: decoder.bytes,
                assignment: decoder.bytes
              )
            }
          else
            # Unknown
            raise "Unknown group metadata value schema #{schema}"
          end
        end
      end
    end

    class GroupTopicPartition
      attr_reader :group, :topic, :partition
      def initialize(group, topic, partition)
        @group     = group
        @topic     = topic
        @partition = partition
      end
    end

    class Group
      attr_reader :group
      def initialize(group)
        @group = group
      end
    end

    class Offset
      attr_reader :offset
      def initialize(offset)
        @offset = offset
      end
    end

    class GroupMetadata
      attr_reader :protocol_type, :generation, :protocol, :leader, :members
      def initialize(protocol_type, generation, protocol, leader, current_state_timestamp = nil, members)
        @protocol_type           = protocol_type
        @generation              = generation
        @protocol                = protocol
        @leader                  = leader
        @current_state_timestamp = current_state_timestamp
        @members                 = members
      end
    end

    class GroupMember
      def initialize(member_id:, group_instance_id: nil, client_id:, client_host:, rebalance_timeout:, session_timeout:, subscription:, assignment:)
        @member_id         = member_id
        @group_instance_id = group_instance_id
        @client_id         = client_id
        @client_host       = client_host
        @rebalance_timeout = rebalance_timeout
        @session_timeout   = session_timeout
      end
    end

  end
end
