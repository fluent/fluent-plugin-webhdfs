# -*- coding: utf-8 -*-

require_relative 'ext_mixin'

class Fluent::WebHDFSOutput < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('webhdfs', self)

  WEBHDFS_VERSION = 'v1'

  config_set_default :buffer_type, 'memory'
  config_set_default :time_slice_format, '%Y%m%d'

  config_param :namenode, :string # host:port
  config_param :path, :string
  config_param :username, :string, :default => nil

  config_param :httpfs, :bool, :default => false

  include FluentExt::PlainTextFormatterMixin
  config_set_default :output_include_time, true
  config_set_default :output_include_tag, true
  config_set_default :output_data_type, 'json'
  config_set_default :field_separator, "\t"
  config_set_default :add_newline, true
  config_set_default :remove_prefix, nil

  def initialize
    super
    require 'net/http'
    require 'time'
    require 'webhdfs'
  end

  def configure(conf)
    if conf['path']
      if conf['path'].index('%S')
        conf['time_slice_format'] = '%Y%m%d%H%M%S'
      elsif conf['path'].index('%M')
        conf['time_slice_format'] = '%Y%m%d%H%M'
      elsif conf['path'].index('%H')
        conf['time_slice_format'] = '%Y%m%d%H'
      end
    end

    super

    unless /\A([a-zA-Z0-9][-a-zA-Z0-9.]*):(\d+)\Z/ =~ @namenode
      raise Fluent::ConfigError, "Invalid config value about namenode: '#{@namenode}', needs NAMENODE_NAME:PORT"
    end
    @namenode_host = $1
    @namenode_port = $2.to_i
    unless @path.index('/') == 0
      raise Fluent::ConfigError, "Path on hdfs MUST starts with '/', but '#{@path}'"
    end
    @conn = nil
    
    @f_separator = case @field_separator
                   when 'SPACE' then ' '
                   when 'COMMA' then ','
                   else "\t"
                   end

    # path => cached_url
    # @cached_datanode_urls = {}
    @client = WebHDFS::Client.new(@namenode_host, @namenode_port, @username)
    if @httpfs
      @client.httpfs_mode = true
    end
    @mutex = Mutex.new
  end

  def start
    super

    noerror = false
    begin
      ary = @client.list('/')
      noerror = true
    rescue
      $log.error "webdhfs check request failed!"
      raise
    end
    $log.info "webhdfs connection confirmed: #{@namenode_host}:#{@namenode_port}"
  end

  def shutdown
    super
  end

  def record_to_string(record)
    record.to_json
  end

  def format(tag, time, record)
    time_str = @timef.format(time)
    time_str + @f_separator + tag + @f_separator + record_to_string(record) + @line_end
  end

  def path_format(chunk_key)
    Time.strptime(chunk_key, @time_slice_format).strftime(@path)
  end

  # TODO datanode url caching?

  # TODO check conflictions
  
  def send_data(path, data)
    begin
      @client.append(path, data)
    rescue WebHDFS::FileNotFoundError
      @client.create(path, data)
    end
  end

  def write(chunk)
    hdfs_path = path_format(chunk.key)
    begin
      send_data(hdfs_path, chunk.read)
    rescue
      $log.error "failed to communicate hdfs cluster, path: #{hdfs_path}"
      raise
    end
    hdfs_path
  end
end
