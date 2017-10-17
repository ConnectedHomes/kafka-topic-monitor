# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = 'kafka-topic-monitor'
  spec.version       = '1.2.0'
  spec.authors       = ['Dmitry Andrianov', 'Talal Al-Tamimi']
  spec.email         = ['dmitry.andrianov@hivehome.com', 'talal.al-tamimi@hivehome.com']

  spec.summary       = 'Consumer lag monitor'
  spec.description   = 'Monitors Kafka consumer lags and emits them as Graphite metrics'
  spec.homepage      = 'https://github.com/ConnectedHomes/kafka-topic-monitor'

  # Prevent pushing this gem to RubyGems.org by setting 'allowed_push_host', or
  # delete this section to allow pushing this gem to any host.
  # if spec.respond_to?(:metadata)
  #   spec.metadata['allowed_push_host'] = 'TODO: Set to 'http://mygemserver.com''
  # else
  #  raise 'RubyGems 2.0 or newer is required to protect against public gem pushes.'
  # end

  spec.required_ruby_version = '~> 2.1'

  spec.files         = Dir['README.md', '{bin,lib,spec}/**/*']
  spec.bindir        = 'bin'
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) } # possible ['verify'] to ensure valid gem ??
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.12'
  spec.add_development_dependency 'rspec', '~> 3.4'
  spec.add_runtime_dependency 'ruby-kafka', '0.4.2'
end
