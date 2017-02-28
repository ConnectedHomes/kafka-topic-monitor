require 'ostruct'
require_relative '../lib/metric_reporter'
 
class SenderMock
  attr_reader :results
  def initialize
    @results = {}
  end
  def publish(time, name_components, value)
    @results[name_components.join('.')] = value
  end
end

class MockTopicDataRetriever
  def get_topic_offsets
    { 'ABC' => { 0 => 100, 1 => 100 } }
  end
end

class MockConsumerDataMonitor
  def start
  end
  def get_consumer_offsets
    { 'XYZ' => { 'ABC' => { 0 => 90, 1 => 99 } } }
  end
end

describe HiveHome::KafkaTopicMonitor::Reporter do
  before(:each) do
    @sender  = SenderMock.new
    @options = OpenStruct.new(
      :report_end_offsets      => false,
      :report_consumer_offsets => false,
      :report_consumer_lag     => :none
    )
    @topic_data_retriever  = MockTopicDataRetriever.new
    @consumer_data_monitor = MockConsumerDataMonitor.new

    @reporter = HiveHome::KafkaTopicMonitor::Reporter.new(@sender, @options)
    @reporter.instance_variable_set(:@data_retriever  , @topic_data_retriever)
    @reporter.instance_variable_set(:@consumer_monitor, @consumer_data_monitor)
  end

  it 'emits no metrics if no options are set' do
    @options.report_end_offsets      = false
    @options.report_consumer_offsets = false
    @options.report_consumer_lag     = :none
    
    @reporter.report
    expect(@sender.results).to be_empty
  end

  it 'emits end offsets' do
    @options.report_end_offsets      = true
    @options.report_consumer_offsets = false
    @options.report_consumer_lag     = :none
    
    @reporter.report

    expect(@sender.results.length).to eq(2)
    expect(@sender.results).to have_key('topic.ABC.partition.0.end_offset')
    expect(@sender.results).to have_key('topic.ABC.partition.1.end_offset')
    expect(@sender.results['topic.ABC.partition.0.end_offset']).to eq(100)
    expect(@sender.results['topic.ABC.partition.1.end_offset']).to eq(100)
  end

  it 'emits consumer offsets' do
    @options.report_end_offsets      = false
    @options.report_consumer_offsets = true
    @options.report_consumer_lag     = :none
    
    @reporter.report

    expect(@sender.results.length).to eq(2)
    expect(@sender.results).to have_key('group.XYZ.topic.ABC.partition.0.consumer_offset')
    expect(@sender.results).to have_key('group.XYZ.topic.ABC.partition.1.consumer_offset')
    expect(@sender.results['group.XYZ.topic.ABC.partition.0.consumer_offset']).to eq(90)
    expect(@sender.results['group.XYZ.topic.ABC.partition.1.consumer_offset']).to eq(99)
  end

  it 'emit partition lags' do
    @options.report_end_offsets      = false
    @options.report_consumer_offsets = false
    @options.report_consumer_lag     = :partition
    
    @reporter.report

    expect(@sender.results.length).to eq(2)
    expect(@sender.results).to have_key('group.XYZ.topic.ABC.partition.0.lag')
    expect(@sender.results).to have_key('group.XYZ.topic.ABC.partition.1.lag')
    expect(@sender.results['group.XYZ.topic.ABC.partition.0.lag']).to eq(10)
    expect(@sender.results['group.XYZ.topic.ABC.partition.1.lag']).to eq(1)
  end

  it 'emit topic lags' do
    @options.report_end_offsets      = false
    @options.report_consumer_offsets = false
    @options.report_consumer_lag     = :total
    
    @reporter.report

    expect(@sender.results.length).to eq(1)
    expect(@sender.results).to have_key('group.XYZ.topic.ABC.total.lag')
    expect(@sender.results['group.XYZ.topic.ABC.total.lag']).to eq(11)
  end

  it 'emit partittion and topic lags' do
    @options.report_end_offsets      = false
    @options.report_consumer_offsets = false
    @options.report_consumer_lag     = :both
    
    @reporter.report

    expect(@sender.results.length).to eq(3)
    expect(@sender.results).to have_key('group.XYZ.topic.ABC.partition.0.lag')
    expect(@sender.results).to have_key('group.XYZ.topic.ABC.partition.1.lag')
    expect(@sender.results).to have_key('group.XYZ.topic.ABC.total.lag')
    expect(@sender.results['group.XYZ.topic.ABC.partition.0.lag']).to eq(10)
    expect(@sender.results['group.XYZ.topic.ABC.partition.1.lag']).to eq(1)
    expect(@sender.results['group.XYZ.topic.ABC.total.lag']).to eq(11)
  end

  # Since consumer offsets and end offsets are queried by different threads at slightly
  # different instants in time, topics and partitions may not match. That can happen,
  # for example, when new topics are getting created at the time. The metrics reporter
  # needs to detect such mismatches and skip sending the corresponding metrics.
  it 'handles missing end offsets gracefully' do
    allow(@topic_data_retriever).to receive(:get_topic_offsets).and_return(
      {
        'A' => { 0 => 100           },
        'B' => { 0 => 100           },
        'C' => { 0 => 100, 1 => 100 },
        'D' => { 0 => 100           }
      }
    )
    allow(@consumer_data_monitor).to receive(:get_consumer_offsets).and_return(
      { 
        'grp' => { 
          'A' => { 0 => 99          },
          'B' => { 0 => 99, 1 => 99 },
          'C' => { 0 => 99          },
          'X' => { 0 => 99          }
        }
      }
    )

    @options.report_end_offsets      = true
    @options.report_consumer_offsets = true
    @options.report_consumer_lag     = :both

    @reporter.report

    expect(@sender.results).to have_key('topic.A.partition.0.end_offset')
    expect(@sender.results).to have_key('topic.B.partition.0.end_offset')
    expect(@sender.results).to have_key('topic.C.partition.0.end_offset')
    expect(@sender.results).to have_key('topic.D.partition.0.end_offset')

    expect(@sender.results).to have_key('group.grp.topic.A.partition.0.consumer_offset')
    expect(@sender.results).to have_key('group.grp.topic.B.partition.0.consumer_offset')
    expect(@sender.results).to have_key('group.grp.topic.B.partition.1.consumer_offset')
    expect(@sender.results).to have_key('group.grp.topic.C.partition.0.consumer_offset')
    expect(@sender.results).to have_key('group.grp.topic.X.partition.0.consumer_offset')

    expect(@sender.results).to     have_key('group.grp.topic.A.partition.0.lag')
    expect(@sender.results).to     have_key('group.grp.topic.B.partition.0.lag')
    expect(@sender.results).not_to have_key('group.grp.topic.B.partition.1.lag')
    expect(@sender.results).to     have_key('group.grp.topic.C.partition.0.lag')
    expect(@sender.results).not_to have_key('group.grp.topic.C.partition.1.lag')
    expect(@sender.results).not_to have_key('group.grp.topic.D.partition.0.lag')
    expect(@sender.results).not_to have_key('group.grp.topic.X.partition.0.lag')
  end

  it 'yields partition offsets' do
    data = {
      'consumer-group-1' => { 'topic-a' => { 0 => 88 } },
      'consumer-group-2' => { 'topic-b' => { 0 => 99 } }
    }
    data_copy = {
      'consumer-group-1' => { 'topic-a' => { 0 => 0 } },
      'consumer-group-2' => { 'topic-b' => { 0 => 0 } }
    }

    call_count = 0
    @reporter.send(:each_paritition, data) do |group, topic, partition, offset|
      call_count += 1
      data_copy[group][topic][partition] = offset  
    end
    
    expect(data_copy).to eq(data)
    expect(call_count).to eq(2)
  end

  it 'yields topic offsets' do
    data = {
      'consumer-group-1' => { 'topic-a' => { 0 => 88 } },
      'consumer-group-2' => { 'topic-b' => { 0 => 99 } }
    }
    expected = {
      'consumer-group-1' => { 'topic-a' => 88 },
      'consumer-group-2' => { 'topic-b' => 99 }
    }
    result = {
      'consumer-group-1' => { 'topic-a' => 0 },
      'consumer-group-2' => { 'topic-b' => 0 }
    }

    call_count = 0
    @reporter.send(:each_topic, data) do |group, topic, offset|
      call_count += 1
      result[group][topic] = offset
    end
    
    expect(result).to eq(expected)
    expect(call_count).to eq(2)
  end

end

