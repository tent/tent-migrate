# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'tent-migrate/version'

Gem::Specification.new do |gem|
  gem.name          = "tent-migrate"
  gem.version       = TentMigrate::VERSION
  gem.authors       = ["Jesse Stuart"]
  gem.email         = ["jessestuart@gmail.com"]
  gem.description   = %q{Move your data from one server to another}
  gem.summary       = %q{Move your data from one server to another}
  gem.homepage      = "https://github.com/tent/tent-migrate"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_runtime_dependency "tent-client"
  gem.add_runtime_dependency "omniauth-tent"
  gem.add_runtime_dependency "sinatra"
  gem.add_runtime_dependency "sprockets"
  gem.add_runtime_dependency "coffee-script"
  gem.add_runtime_dependency "sass"
  gem.add_runtime_dependency "redis"
  gem.add_runtime_dependency "yajl-ruby"
  gem.add_runtime_dependency "girl_friday"
  gem.add_runtime_dependency "airbrake"
end
