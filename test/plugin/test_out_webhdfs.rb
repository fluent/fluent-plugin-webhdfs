require 'helper'

class WebHDFSOutputTest < Test::Unit::TestCase
  CONFIG = config_element(
    "ROOT", "", {
      "host" => "namenode.local",
      "path" => "/hdfs/path/file.%Y%m%d.log"
    })

  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::WebHDFSOutput).configure(conf)
  end

  class ConfigureTest < self
    def test_default
      d = create_driver
      assert_equal 'namenode.local', d.instance.instance_eval{ @namenode_host }
      assert_equal 50070, d.instance.instance_eval{ @namenode_port }
      assert_equal '/hdfs/path/file.%Y%m%d.log', d.instance.path
      assert_equal false, d.instance.httpfs
      assert_nil d.instance.username
      assert_equal false, d.instance.ignore_start_check_error

      assert_equal true, d.instance.output_include_time
      assert_equal true, d.instance.output_include_tag
      assert_equal 'json', d.instance.output_data_type
      assert_nil d.instance.remove_prefix
      assert_equal 'TAB', d.instance.field_separator
      assert_equal true, d.instance.add_newline
      assert_equal 'tag_missing', d.instance.default_tag
    end

    def test_httpfs
      conf = config_element(
        "ROOT", "", {
          "namenode" => "server.local:14000",
          "path" => "/hdfs/path/file.%Y%m%d.%H%M.log",
          "httpfs" => "yes",
          "username" => "hdfs_user"
        })
      d = create_driver(conf)

      assert_equal 'server.local', d.instance.instance_eval{ @namenode_host }
      assert_equal 14000, d.instance.instance_eval{ @namenode_port }
      assert_equal '/hdfs/path/file.%Y%m%d.%H%M.log', d.instance.path
      assert_equal true, d.instance.httpfs
      assert_equal 'hdfs_user', d.instance.username
    end

    def test_ssl
      conf = config_element(
        "ROOT", "", {
          "namenode" => "server.local:14000",
          "path" => "/hdfs/path/file.%Y%m%d.%H%M.log",
          "ssl" => true,
          "ssl_ca_file" => "/path/to/ca_file.pem",
          "ssl_verify_mode" => "peer",
          "kerberos" => true
        })
      d = create_driver(conf)

      assert_equal 'server.local', d.instance.instance_eval{ @namenode_host }
      assert_equal 14000, d.instance.instance_eval{ @namenode_port }
      assert_equal '/hdfs/path/file.%Y%m%d.%H%M.log', d.instance.path
      assert_equal true, d.instance.ssl
      assert_equal '/path/to/ca_file.pem', d.instance.ssl_ca_file
      assert_equal :peer, d.instance.ssl_verify_mode
      assert_equal true, d.instance.kerberos
    end

    data(gzip: ['gzip', Fluent::Plugin::WebHDFSOutput::GzipCompressor],
         bzip2: ['bzip2', Fluent::Plugin::WebHDFSOutput::Bzip2Compressor],
         snappy: ['snappy', Fluent::Plugin::WebHDFSOutput::SnappyCompressor],
         lzo: ['lzo_command', Fluent::Plugin::WebHDFSOutput::LZOCommandCompressor])
    def test_compress(data)
      compress_type, compressor_class = data
      begin
        conf = config_element(
          "ROOT", "", {
            "namenode" => "server.local:14000",
            "path" => "/hdfs/path/file.%Y%m%d.%H%M.log",
            "compress" => compress_type
          })
        d = create_driver(conf)
      rescue Fluent::ConfigError => ex
        omit ex.message
      end
      assert_equal 'server.local', d.instance.instance_eval{ @namenode_host }
      assert_equal 14000, d.instance.instance_eval{ @namenode_port }
      assert_equal '/hdfs/path/file.%Y%m%d.%H%M.log', d.instance.path
      assert_equal compress_type, d.instance.compress
      assert_equal compressor_class, d.instance.compressor.class
    end

    def test_placeholders
      conf = config_element(
        "ROOT", "", {
          "hostname" => "testing.node.local",
          "namenode" => "server.local:50070",
          "path" => "/hdfs/${hostname}/file.%Y%m%d%H.log"
        })
      d = create_driver(conf)
      assert_equal '/hdfs/testing.node.local/file.%Y%m%d%H.log', d.instance.path
    end

    class PathFormatTest < self
      def test_default
        d = create_driver
        time = event_time("2012-07-18 15:03:00 +0900")
        metadata = d.instance.metadata("test", time, {})
        chunk = d.instance.buffer.generate_chunk(metadata)
        assert_equal '/hdfs/path/file.%Y%m%d.log', d.instance.path
        assert_equal '/hdfs/path/file.20120718.log', d.instance.generate_path(chunk)
      end

      def test_time_slice_format
        conf = config_element(
          "ROOT", "", {
            "namenode" => "server.local:14000",
            "path" => "/hdfs/path/file.%Y%m%d.%H%M.log"
          })
        d = create_driver(conf)

        time = event_time("2012-07-18 15:03:00 +0900")
        metadata = d.instance.metadata("test", time, {})
        chunk = d.instance.buffer.generate_chunk(metadata)
        assert_equal '/hdfs/path/file.%Y%m%d.%H%M.log', d.instance.path
        assert_equal '/hdfs/path/file.20120718.1503.log', d.instance.generate_path(chunk)
      end
    end

    data(path: { "append" => false },
         ssl: { "ssl" => true, "ssl_verify_mode" => "invalid" },
         compress: { "compress" => "invalid" })
    test "invalid" do |attr|
      conf = config_element(
        "ROOT", "", {
          "namenode" => "server.local:14000",
          "path" => "/hdfs/path/file.%Y%m%d.%H%M.log"
        })
      conf += config_element("", "", attr)
      assert_raise Fluent::ConfigError do
        create_driver(conf)
      end
    end
  end
end
