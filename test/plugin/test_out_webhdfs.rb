require 'helper'

class WebHDFSOutputTest < Test::Unit::TestCase
  CONFIG_DEFAULT = config_element("match", "", {"host" => "namenode.local", "path" => "/hdfs/path/file.%Y%m%d.log"})

  CONFIG_COMPAT = config_element(
    "ROOT", "", {
      "output_data_type" => "",
      "host" => "namenode.local",
      "path" => "/hdfs/path/file.%Y%m%d.log"
    })

  def setup
    Fluent::Test.setup
  end

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::WebHDFSOutput).configure(conf)
  end

  sub_test_case "default configuration" do
    test 'configured with standard out_file format with specified hdfs info' do
      d = create_driver(CONFIG_DEFAULT)
      assert_true d.instance.instance_eval{ @using_formatter_config }

      assert_equal 'namenode.local', d.instance.instance_eval{ @namenode_host }
      assert_equal 50070, d.instance.instance_eval{ @namenode_port }
      assert_equal '/hdfs/path/file.%Y%m%d.log', d.instance.path
      assert_equal false, d.instance.httpfs
      assert_nil d.instance.username
      assert_equal false, d.instance.ignore_start_check_error

      assert_equal 'Fluent::Plugin::OutFileFormatter', d.instance.formatter.class.to_s
      assert_equal true, d.instance.end_with_newline

      # deprecated params
      assert_nil d.instance.instance_eval{ @output_include_time }
      assert_nil d.instance.instance_eval{ @output_include_tag }
      assert_nil d.instance.remove_prefix
      assert_nil d.instance.instance_eval{ @header_separator }
      assert_nil d.instance.default_tag
    end
  end

  sub_test_case "flat configuration" do
    def test_default_for_traditional_config
      d = create_driver(CONFIG_COMPAT)
      assert_false d.instance.instance_eval{ @using_formatter_config }

      assert_equal 'namenode.local', d.instance.instance_eval{ @namenode_host }
      assert_equal 50070, d.instance.instance_eval{ @namenode_port }
      assert_equal '/hdfs/path/file.%Y%m%d.log', d.instance.path
      assert_equal false, d.instance.httpfs
      assert_nil d.instance.username
      assert_equal false, d.instance.ignore_start_check_error

      assert_equal 'Fluent::Plugin::JSONFormatter', d.instance.formatter.class.to_s
      assert_equal true, d.instance.end_with_newline

      assert_equal true, d.instance.instance_eval{ @output_include_time }
      assert_equal true, d.instance.instance_eval{ @output_include_tag }
      assert_nil d.instance.instance_eval{ @remove_prefix }
      assert_equal "\t", d.instance.instance_eval{ @header_separator }
      assert_equal 'tag_missing', d.instance.instance_eval{ @default_tag }
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
          "kerberos" => true,
          "kerberos_keytab" => "/path/to/kerberos.keytab"
        })
      d = create_driver(conf)

      assert_equal 'server.local', d.instance.instance_eval{ @namenode_host }
      assert_equal 14000, d.instance.instance_eval{ @namenode_port }
      assert_equal '/hdfs/path/file.%Y%m%d.%H%M.log', d.instance.path
      assert_equal true, d.instance.ssl
      assert_equal '/path/to/ca_file.pem', d.instance.ssl_ca_file
      assert_equal :peer, d.instance.ssl_verify_mode
      assert_equal true, d.instance.kerberos
      assert_equal '/path/to/kerberos.keytab', d.instance.kerberos_keytab
    end

    data(gzip: [:gzip, Fluent::Plugin::WebHDFSOutput::GzipCompressor],
         bzip2: [:bzip2, Fluent::Plugin::WebHDFSOutput::Bzip2Compressor],
         snappy: [:snappy, Fluent::Plugin::WebHDFSOutput::SnappyCompressor],
         hadoop_snappy: [:hadoop_snappy, Fluent::Plugin::WebHDFSOutput::HadoopSnappyCompressor],
         lzo: [:lzo_command, Fluent::Plugin::WebHDFSOutput::LZOCommandCompressor])
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

      time = event_time("2020-10-03 15:07:00 +0300")
      metadata = d.instance.metadata("test", time, {})
      chunk = d.instance.buffer.generate_chunk(metadata)
      assert_equal "/hdfs/path/file.20201003.1507.log#{d.instance.compressor.ext}", d.instance.generate_path(chunk)
    end

    def test_explicit_extensions
      conf = config_element(
        "ROOT", "", {
          "host" => "namenode.local",
          "path" => "/hdfs/path/file.%Y%m%d.log",
          "compress" => "snappy",
          "extension" => ".snappy"
        })
      d = create_driver(conf)
      time = event_time("2020-10-07 15:15:00 +0300")
      metadata = d.instance.metadata("test", time, {})
      chunk = d.instance.buffer.generate_chunk(metadata)
      assert_equal "/hdfs/path/file.20201007.log.snappy", d.instance.generate_path(chunk)
    end

    data(snappy: [:snappy, Fluent::Plugin::WebHDFSOutput::SnappyCompressor],
         hadoop_snappy: [:hadoop_snappy, Fluent::Plugin::WebHDFSOutput::HadoopSnappyCompressor])
    def test_compression_block_size(data)
      compress_type, compressor_class = data
      conf = config_element(
        "ROOT", "", {
          "host" => "namenode.local",
          "path" => "/hdfs/path/file.%Y%m%d.log",
          "compress" => compress_type,
          "block_size" => 16384
        })
      d = create_driver(conf)

      assert_equal compress_type, d.instance.compress
      assert_equal 16384, d.instance.compressor.block_size
    end

    def test_placeholders_old_style
      conf = config_element(
        "ROOT", "", {
          "hostname" => "testing.node.local",
          "namenode" => "server.local:50070",
          "path" => "/hdfs/${hostname}/file.%Y%m%d%H.log"
        })
      d = create_driver(conf)
      assert_equal '/hdfs/testing.node.local/file.%Y%m%d%H.log', d.instance.path
    end

    data("%Y%m%d" => ["/hdfs/path/file.%Y%m%d.log", "/hdfs/path/file.20120718.log"],
         "%Y%m%d.%H%M" => ["/hdfs/path/file.%Y%m%d.%H%M.log", "/hdfs/path/file.20120718.1503.log"])
    test "generate_path" do |(path, expected)|
      conf = config_element(
        "ROOT", "", {
          "namenode" => "server.local:14000",
          "path" => path
        })
      d = create_driver(conf)
      formatter = Fluent::Timezone.formatter("+0900", path)
      mock(Fluent::Timezone).formatter(Time.now.strftime("%z"), path) { formatter }
      time = event_time("2012-07-18 15:03:00 +0900")
      metadata = d.instance.metadata("test", time, {})
      chunk = d.instance.buffer.generate_chunk(metadata)
      assert_equal expected, d.instance.generate_path(chunk)
    end

    data(path: "/hdfs/path/file.${chunk_id}.log")
    test "generate_path without append" do |path|
      conf = config_element(
        "ROOT", "", {
          "namenode" => "server.local:14000",
          "path" => path,
          "append" => false
        })
      d = create_driver(conf)
      metadata = d.instance.metadata("test", nil, {})
      chunk = d.instance.buffer.generate_chunk(metadata)
      assert_equal "/hdfs/path/file.#{dump_unique_id_hex(chunk.unique_id)}.log", d.instance.generate_path(chunk)
      assert_empty d.instance.log.out.logs
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

  sub_test_case "sub section configuration" do
    def test_time_key
      conf = config_element(
        "ROOT", "", {
          "host" => "namenode.local",
          "path" => "/hdfs/path/file.%Y%m%d.log"
        }, [
          config_element(
            "buffer", "time", {
              "timekey" => 1
            })
        ]
      )
      d = create_driver(conf)
      time = event_time("2012-07-18 15:03:00 +0900")
      metadata = d.instance.metadata("test", time, {})
      chunk = d.instance.buffer.generate_chunk(metadata)
      assert_equal 1, d.instance.buffer_config.timekey
      assert_equal "/hdfs/path/file.20120718.log", d.instance.generate_path(chunk)
    end

    def test_time_key_without_buffer_section
      conf = config_element(
        "ROOT", "", {
          "host" => "namenode.local",
          "path" => "/hdfs/path/file.%Y%m%d-%M.log"
        }
      )
      d = create_driver(conf)
      time = event_time("2012-07-18 15:03:00 +0900")
      metadata = d.instance.metadata("test", time, {})
      chunk = d.instance.buffer.generate_chunk(metadata)
      assert_equal 60, d.instance.buffer_config.timekey
      assert_equal "/hdfs/path/file.20120718-03.log", d.instance.generate_path(chunk)
    end
  end

  sub_test_case "using format subsection" do
    test "blank format means default format 'out_file' with UTC timezone" do
      format_section = config_element("format", "", {}, [])
      conf = config_element("match", "", {"host" => "namenode.local", "path" => "/hdfs/path/file.%Y%m%d%H.log"}, [format_section])
      d = create_driver(conf)
      time = event_time("2017-01-24 13:10:30 -0700")
      line = d.instance.format("test.now", time, {"message" => "yay", "name" => "tagomoris"})
      assert_equal "2017-01-24T20:10:30Z\ttest.now\t{\"message\":\"yay\",\"name\":\"tagomoris\"}\n", line
    end

    test "specifying timezone works well in format section" do
      format_section = config_element("format", "", {"timezone" => "+0100"}, [])
      conf = config_element("match", "", {"host" => "namenode.local", "path" => "/hdfs/path/file.%Y%m%d%H.log"}, [format_section])
      d = create_driver(conf)
      time = event_time("2017-01-24 13:10:30 -0700")
      line = d.instance.format("test.now", time, {"message" => "yay", "name" => "tagomoris"})
      assert_equal "2017-01-24T21:10:30+01:00\ttest.now\t{\"message\":\"yay\",\"name\":\"tagomoris\"}\n", line
    end

    test "specifying formatter type LTSV for records, without tag and timezone" do
      format_section = config_element("format", "", {"@type" => "ltsv"}, [])
      conf = config_element("match", "", {"host" => "namenode.local", "path" => "/hdfs/path/file.%Y%m%d%H.log"}, [format_section])
      d = create_driver(conf)
      time = event_time("2017-01-24 13:10:30 -0700")
      line = d.instance.format("test.now", time, {"message" => "yay", "name" => "tagomoris"})
      assert_equal "message:yay\tname:tagomoris\n", line
    end

    test "specifying formatter type LTSV for records, with inject section to insert tag and time" do
      inject_section = config_element("inject", "", {"tag_key" => "tag", "time_key" => "time", "time_type" => "string", "localtime" => "false"})
      format_section = config_element("format", "", {"@type" => "ltsv"}, [])
      conf = config_element("match", "", {"host" => "namenode.local", "path" => "/hdfs/path/file.%Y%m%d%H.log"}, [inject_section, format_section])
      d = create_driver(conf)
      time = event_time("2017-01-24 13:10:30 -0700")
      line = d.instance.format("test.now", time, {"message" => "yay", "name" => "tagomoris"})
      assert_equal "message:yay\tname:tagomoris\ttag:test.now\ttime:2017-01-24T20:10:30Z\n", line
    end
  end

  sub_test_case "using older configuration" do
    test "output_data_type json is same with out_file with UTC timezone" do
      conf = config_element("match", "", {"host" => "namenode.local", "path" => "/hdfs/path/file.%Y%m%d%H.log", "output_data_type" => "json"}, [])
      d = create_driver(conf)
      time = event_time("2017-01-24 13:10:30 -0700")
      line = d.instance.format("test.now", time, {"message" => "yay", "name" => "tagomoris"})
      assert_equal "2017-01-24T20:10:30Z\ttest.now\t{\"message\":\"yay\",\"name\":\"tagomoris\"}\n", line
    end
  end
end
