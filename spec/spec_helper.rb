require 'rubygems'
require 'bundler/setup'

ENV['RAILS_ENV'] ||= 'development'

Bundler.require(:default, ENV['RAILS_ENV'])

require File.expand_path('../../lib/pg_helper', __FILE__)