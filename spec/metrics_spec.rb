require_relative '../lib/metrics'

describe HiveHome::KafkaTopicMonitor::Metrics do
  before(:each) do
    @metrics = HiveHome::KafkaTopicMonitor::Metrics.new
  end

  it 'increments counters' do
    @metrics.increment(['counter1'])
    @metrics.increment(['path', 'counter2'])
    @metrics.increment(['counter1'])

    result = @metrics.get_metrics
    expect(result[['counter1']]).to eq(2)
    expect(result[['path', 'counter2']]).to eq(1)
  end

  it 'provides stats for a single invocations' do
    timer = @metrics.timer(['timer']).start
    sleep 0.1
    timer.stop

    result = @metrics.get_metrics
    expect(result[['timer', 'count']]).to eq(1)
    avg = result[['timer', 'avg']]
    expect(avg).to be >= 100
    # With a single invocation, all min/max metrics are of that invocation
    expect(result[['timer', 'min']]).to eq(avg)
    expect(result[['timer', 'max']]).to eq(avg)
  end

  it 'provides stats for timer invocations' do
    timer1 = @metrics.timer(['timer']).start
    timer2 = @metrics.timer(['timer']).start
    timer3 = @metrics.timer(['timer']).start
    sleep 0.1
    timer1.stop
    sleep 0.1
    timer2.stop
    sleep 0.1
    timer3.stop

    result = @metrics.get_metrics
    expect(result[['timer', 'count']]).to eq(3)
    avg = result[['timer', 'avg']]
    min = result[['timer', 'min']]
    max = result[['timer', 'max']]
    expect(min).to be >= 100
    expect(avg).to be >= 150
    expect(max).to be >= 300
    expect(min).to be < avg
    expect(max).to be > avg
  end
end

