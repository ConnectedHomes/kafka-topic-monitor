require 'ostruct'
require 'stringio'
require_relative '../lib/consumer_monitor'

class MockKafkaClient
  attr_accessor :started

  def initialize
    @subscribed = false
    @running = true
    @queue = Queue.new
  end

  def each_message(topic:, start_from_beginning: true, max_wait_time: 5, min_bytes: 1, max_bytes: 1048576, &block)
    @subscribed = true
    while @running do
      message = @queue.pop
      block.call(message) if @running
    end     
  end

  def inject(message)
    @queue.push message
  end

  def wait_until_subscribed
    while not @subscribed
      sleep 0.01
    end
  end

  def wait_until_processed
    while not @queue.empty?
      sleep 0.01
    end
  end

  def shutdown
    @running = false
    inject nil
  end
end

describe HiveHome::KafkaTopicMonitor::ConsumerDataMonitor do

  before(:each) do
    @mock_kafka_client = MockKafkaClient.new
    @consumer_data_monitor = HiveHome::KafkaTopicMonitor::ConsumerDataMonitor.new(@mock_kafka_client)
    @consumer_data_monitor.start
    @mock_kafka_client.wait_until_subscribed
  end

  after(:each) do
    @mock_kafka_client.shutdown
  end

  it 'saves last offset' do
    @mock_kafka_client.inject(new_consumer_offset_message('group1', 'topic1', 0, 97))
    @mock_kafka_client.inject(new_consumer_offset_message('group1', 'topic1', 0, 99))
    @mock_kafka_client.wait_until_processed

    expect(@consumer_data_monitor.get_consumer_offsets['group1']['topic1'][0]).to eq(99)
  end

  it 'removes topic for nil offset' do
    @mock_kafka_client.inject(new_consumer_offset_message('group1', 'topic1', 0, 97))
    @mock_kafka_client.inject(new_consumer_offset_message('group1', 'topic1', 0, nil))
    @mock_kafka_client.wait_until_processed

    expect(@consumer_data_monitor.get_consumer_offsets['group1']).to eq(nil)
  end

  it 'handles un-registered deleted topic' do
    @mock_kafka_client.inject(new_consumer_offset_message('group1', 'topic1', 0, nil))
    @mock_kafka_client.wait_until_processed

    expect(@consumer_data_monitor.get_consumer_offsets['group1']).to eq(nil)
  end

end

def new_consumer_offset_message(group, topic, partition, offset)
    key_io  = StringIO.new
    key_enc = ::Kafka::Protocol::Encoder.new(key_io)
    key_enc.write_int16(0) # schema
    key_enc.write_string(group)
    key_enc.write_string(topic)
    key_enc.write_int32(partition)
    message_key = key_io.string

    message_value = nil

    if !offset.nil?
      value_io  = StringIO.new
      value_enc = ::Kafka::Protocol::Encoder.new(value_io)
      value_enc.write_int16(0) # schema
      value_enc.write_int64(offset)
      message_value = value_io.string
    end

    return OpenStruct.new(:key => message_key, :value => message_value)
end
