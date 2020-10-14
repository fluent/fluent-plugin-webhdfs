# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.name          = "fluent-plugin-webhdfs"
  gem.version       = "1.2.5"
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
  gem.add_development_dependency "test-unit-rr"
  gem.add_development_dependency "appraisal"
  gem.add_development_dependency "snappy", '>= 0.0.13'
  gem.add_development_dependency "bzip2-ffi"
  gem.add_development_dependency "zstandard"
  gem.add_runtime_dependency "fluentd", '>= 0.14.22'
  gem.add_runtime_dependency "webhdfs", '>= 0.6.0'
end
