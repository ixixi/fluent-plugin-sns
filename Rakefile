require 'rake'
require 'rake/testtask'
require 'rake/clean'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "fluent-plugin-sns"
    gemspec.summary = "Amazon SNS output plugin for Fluent event collector"
    gemspec.author = "Yuri Odagiri"
    gemspec.email = "ixixizko@gmail.com"
    gemspec.homepage = "https://github.com/ixixi/fluent-plugin-sns"
    gemspec.has_rdoc = false
    gemspec.require_paths = ["lib"]
    gemspec.add_dependency "fluentd", "~> 0.10.0"
    gemspec.add_dependency "aws-sdk-v1", "~> 1"
    gemspec.test_files = Dir["test/**/*.rb"]
    gemspec.files = Dir["lib/**/*", "test/**/*.rb"] + %w[VERSION AUTHORS Rakefile]
    gemspec.executables = []
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end

Rake::TestTask.new(:test) do |t|
  t.test_files = Dir['test/*_test.rb']
  t.ruby_opts = ['-rubygems'] if defined? Gem
  t.ruby_opts << '-I.'
end

task :default => [:build]
