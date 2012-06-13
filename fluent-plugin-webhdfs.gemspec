# -*- encoding: utf-8 -*-
Gem::Specification.new do |gem|
  gem.name          = "fluent-plugin-webhdfs"
  gem.version       = "0.0.2"
  gem.authors       = ["TAGOMORI Satoshi"]
  gem.email         = ["tagomoris@gmail.com"]
  gem.summary       = %q{Fluentd plugin to write data on HDFS over WebHDFS, with flexible formatting}
  gem.description   = %q{For WebHDFS (and HttpFs experimentally)}
  gem.homepage      = "https://github.com/tagomoris/fluent-plugin-webhdfs"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency "rake"
  gem.add_development_dependency "fluentd"
  gem.add_development_dependency "webhdfs", '>= 0.5.0'
  gem.add_runtime_dependency "fluentd"
  gem.add_runtime_dependency "webhdfs", '>= 0.5.0'
end
