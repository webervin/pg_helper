require 'rubygems'
require 'bundler/setup'
require 'rspec'
require File.expand_path('../../lib/pg_helper', __FILE__)

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end