require 'helper'

class WebHDFSOutputTest < Test::Unit::TestCase
  CONFIG = %[
host namenode.local
path /hdfs/path/file.%Y%m%d.log
  ]

  def create_driver(conf=CONFIG,tag='test')
    Fluent::Test::OutputTestDriver.new(Fluent::WebHDFSOutput, tag).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 'namenode.local', d.instance.instance_eval{ @namenode_host }
    assert_equal 50070, d.instance.instance_eval{ @namenode_port }
    assert_equal '/hdfs/path/file.%Y%m%d.log', d.instance.path
    assert_equal '%Y%m%d', d.instance.time_slice_format
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

    d = create_driver %[
namenode server.local:14000
path /hdfs/path/file.%Y%m%d.%H%M.log
httpfs yes
username hdfs_user
]
    assert_equal 'server.local', d.instance.instance_eval{ @namenode_host }
    assert_equal 14000, d.instance.instance_eval{ @namenode_port }
    assert_equal '/hdfs/path/file.%Y%m%d.%H%M.log', d.instance.path
    assert_equal '%Y%m%d%H%M', d.instance.time_slice_format
    assert_equal true, d.instance.httpfs
    assert_equal 'hdfs_user', d.instance.username
  end

  def test_configure_placeholders
    d = create_driver %[
hostname testing.node.local
namenode server.local:50070
path /hdfs/${hostname}/file.%Y%m%d%H.log
]
    assert_equal '/hdfs/testing.node.local/file.%Y%m%d%H.log', d.instance.path
  end

  def test_path_format
    d = create_driver
    assert_equal '/hdfs/path/file.%Y%m%d.log', d.instance.path
    assert_equal '%Y%m%d', d.instance.time_slice_format
    assert_equal '/hdfs/path/file.20120718.log', d.instance.path_format('20120718')

    d = create_driver %[
namenode server.local:14000
path /hdfs/path/file.%Y%m%d.%H%M.log
]
    assert_equal '/hdfs/path/file.%Y%m%d.%H%M.log', d.instance.path
    assert_equal '%Y%m%d%H%M', d.instance.time_slice_format
    assert_equal '/hdfs/path/file.20120718.1503.log', d.instance.path_format('201207181503')
  end
end
