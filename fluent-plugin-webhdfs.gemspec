# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.name          = "fluent-plugin-webhdfs"
  gem.version       = "0.4.1"
  gem.authors       = ["TAGOMORI Satoshi"]
  gem.email         = ["tagomoris@gmail.com"]
  gem.summary       = %q{Fluentd plugin to write data on HDFS over WebHDFS, with flexible formatting}
  gem.description   = %q{For WebHDFS and HttpFs of Hadoop HDFS}
  gem.homepage      = "https://github.com/fluent/fluent-plugin-webhdfs"
  gem.license       = "Apache-2.0"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency "rake"
  gem.add_development_dependency "test-unit"
  gem.add_runtime_dependency "fluentd", '>= 0.10.53'
  gem.add_runtime_dependency "fluent-mixin-plaintextformatter", '>= 0.2.1'
  gem.add_runtime_dependency "fluent-mixin-config-placeholders", ">= 0.3.0"
  gem.add_runtime_dependency "webhdfs", '>= 0.6.0'
end
