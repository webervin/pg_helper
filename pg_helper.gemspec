# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "pg_helper/version"

Gem::Specification.new do |s|
  s.name        = "pg_helper"
  s.version     = PgHelper::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors = ["Ervin"]
  s.email = %q{webervin@gmail.com}
  s.homepage = %q{http://github.com/webervin/pg_helper}
  s.rubyforge_project = "pg_helper"
  s.summary = %q{Tiny wraper for 'pg' gem}
  s.description = %q{Makes even easier to use postgresql without activerecord}
  s.has_rdoc = 'yard'

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_runtime_dependency 'pg'

  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'wirble'
  s.add_development_dependency 'yard'
  s.add_development_dependency 'bluecloth' #yard hidden dependency
end