require 'graphite_sender'

HOST = 'some.nonexistent.server'
PORT = '999999'
BASE = 'example.metric.base'

describe 'GraphiteSender' do
  it 'publishes a metric in graphite format' do
    socket_mock = TCPSocketMock.new
    expect(TCPSocket).to receive(:new).with(HOST, PORT).and_return(socket_mock)
    
    sender = HiveHome::GraphiteSender.new("#{HOST}:#{PORT}", BASE)
    now    = Time.new
    
    sender.publish(now, ['some','metric'], 100)
    expect(socket_mock.last_message).to eq("#{BASE}.some.metric 100 #{now.to_i}")
  end
end

class TCPSocketMock
  attr_reader :last_message
  def puts(s)
    @last_message = s
  end
end
