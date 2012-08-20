# -*- coding: utf-8 -*-

require 'fluent/mixin/config_placeholders'
require 'fluent/mixin/plaintextformatter'

class Fluent::WebHDFSOutput < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('webhdfs', self)

  config_set_default :buffer_type, 'memory'
  config_set_default :time_slice_format, '%Y%m%d'

  config_param :host, :string, :default => nil
  config_param :port, :integer, :default => 50070
  config_param :namenode, :string, :default => nil # host:port

  include Fluent::Mixin::ConfigPlaceholders

  config_param :path, :string
  config_param :username, :string, :default => nil

  config_param :httpfs, :bool, :default => false

  config_param :open_timeout, :integer, :default => 30 # from ruby net/http default
  config_param :read_timeout, :integer, :default => 60 # from ruby net/http default

  include Fluent::Mixin::PlainTextFormatter

  config_param :default_tag, :string, :default => 'tag_missing'

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

    if @host
      @namenode_host = @host
      @namenode_port = @port
    elsif @namenode
      unless /\A([a-zA-Z0-9][-a-zA-Z0-9.]*):(\d+)\Z/ =~ @namenode
        raise Fluent::ConfigError, "Invalid config value about namenode: '#{@namenode}', needs NAMENODE_NAME:PORT"
      end
      @namenode_host = $1
      @namenode_port = $2.to_i
    else
      raise Fluent::ConfigError, "WebHDFS host or namenode missing"
    end
    unless @path.index('/') == 0
      raise Fluent::ConfigError, "Path on hdfs MUST starts with '/', but '#{@path}'"
    end
    
    @client = WebHDFS::Client.new(@namenode_host, @namenode_port, @username)
    if @httpfs
      @client.httpfs_mode = true
    end
    @client.open_timeout = @open_timeout
    @client.read_timeout = @read_timeout
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

  def path_format(chunk_key)
    Time.strptime(chunk_key, @time_slice_format).strftime(@path)
  end

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
