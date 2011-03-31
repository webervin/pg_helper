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

require 'bundler'
Bundler::GemHelper.install_tasks