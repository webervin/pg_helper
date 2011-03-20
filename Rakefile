require 'rubygems'
require 'bundler/setup'

ENV['RAILS_ENV'] ||= 'development'

Bundler.require(:default, ENV['RAILS_ENV'])

require 'lib/pg_helper'

require 'rake'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = ["--color"]
end
task :default  => :spec

require 'jeweler'
Jeweler::Tasks.new do |gem|
  # gem is a Gem::Specification... see http://docs.rubygems.org/read/chapter/20 for more options
  gem.name = "pg_helper"
  gem.homepage = "http://github.com/webervin/pg_helper"
  gem.license = "MIT"
  gem.summary = "Tiny wraper for 'pg' gem"
  gem.description = "Makes even easier to use postgresql without activerecord"
  gem.email = "webervin@gmail.com"
  gem.authors = ["Ervin"]
  # Include your dependencies below. Runtime dependencies are required when using your gem,
  # and development dependencies are only needed for development (ie running rake tasks, tests, etc)
    gem.add_runtime_dependency 'pg', '~> 0.10'
    gem.add_development_dependency 'rspec'
    gem.add_development_dependency 'rake'
    gem.add_development_dependency 'wirble'
    gem.add_development_dependency 'metric_fu'
    gem.add_development_dependency 'ZenTest'
    gem.add_development_dependency 'jeweler'
end
Jeweler::RubygemsDotOrgTasks.new
