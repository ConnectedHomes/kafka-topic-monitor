require 'ostruct'
require 'stringio'
require_relative '../lib/consumer_monitor'

describe HiveHome::KafkaTopicMonitor::ConsumerDataMonitor do

  it 'saves last offset' do
    mock_kafka_client = Object.new
    allow(mock_kafka_client).to receive(:each_message)
        .and_yield(new_consumer_offset_message('group1', 'topic1', 0, 97))
        .and_yield(new_consumer_offset_message('group1', 'topic1', 0, 99))

    consumer_data_monitor = HiveHome::KafkaTopicMonitor::ConsumerDataMonitor.new(mock_kafka_client)
    thread = consumer_data_monitor.start
    sleep 0.1
    expect(consumer_data_monitor.get_consumer_offsets['group1']['topic1'][0]).to eq(99)
    thread.exit
  end

  it 'remove topic for nil offset' do
    mock_kafka_client = Object.new
    allow(mock_kafka_client).to receive(:each_message)
        .and_yield(new_consumer_offset_message('group1', 'topic1', 0, 97))
        .and_yield(new_consumer_offset_message('group1', 'topic1', 0, nil))

    consumer_data_monitor = HiveHome::KafkaTopicMonitor::ConsumerDataMonitor.new(mock_kafka_client)
    thread = consumer_data_monitor.start
    sleep 0.1
    expect(consumer_data_monitor.get_consumer_offsets['group1']['topic1']).to eq(nil)
    thread.exit
  end

  it 'handle un-registered deleted topic' do
    mock_kafka_client = Object.new
    allow(mock_kafka_client).to receive(:each_message)
        .and_yield(new_consumer_offset_message('group1', 'topic1', 0, nil))
    
    consumer_data_monitor = HiveHome::KafkaTopicMonitor::ConsumerDataMonitor.new(mock_kafka_client)
    thread = consumer_data_monitor.start
    sleep 0.1
    expect(consumer_data_monitor.get_consumer_offsets['group1']).to eq(nil)
    thread.exit
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
