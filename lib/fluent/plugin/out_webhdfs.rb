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
  config_param :standby_namenode, :string, :default => nil # host:port

  config_param :ignore_start_check_error, :bool, :default => false

  include Fluent::Mixin::ConfigPlaceholders

  config_param :path, :string
  config_param :username, :string, :default => nil

  config_param :httpfs, :bool, :default => false

  config_param :open_timeout, :integer, :default => 30 # from ruby net/http default
  config_param :read_timeout, :integer, :default => 60 # from ruby net/http default

  config_param :retry_known_errors, :bool, :default => false
  config_param :retry_interval, :integer, :default => nil
  config_param :retry_times, :integer, :default => nil

  # how many times of write failure before switch to standby namenode
  # by default it's 11 times that costs 1023 seconds inside fluentd,
  # which is considered enough to exclude the scenes that caused by temporary network fail or single datanode fail
  config_param :failures_before_use_standby, :integer, :default => 11

  include Fluent::Mixin::PlainTextFormatter

  config_param :default_tag, :string, :default => 'tag_missing'

  config_param :append, :bool, :default => true

  CHUNK_ID_PLACE_HOLDER = '${chunk_id}'

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
        raise Fluent::ConfigError, "Invalid config value about namenode: '#{@namenode}', needs NAMENODE_HOST:PORT"
      end
      @namenode_host = $1
      @namenode_port = $2.to_i
    else
      raise Fluent::ConfigError, "WebHDFS host or namenode missing"
    end
    if @standby_namenode
      unless /\A([a-zA-Z0-9][-a-zA-Z0-9.]*):(\d+)\Z/ =~ @standby_namenode
        raise Fluent::ConfigError, "Invalid config value about standby namenode: '#{@standby_namenode}', needs STANDBY_NAMENODE_HOST:PORT"
      end
      if @httpfs
        raise Fluent::ConfigError, "Invalid configuration: specified to use both of standby_namenode and httpfs."
      end
      @standby_namenode_host = $1
      @standby_namenode_port = $2.to_i
    end
    unless @path.index('/') == 0
      raise Fluent::ConfigError, "Path on hdfs MUST starts with '/', but '#{@path}'"
    end

    @client = prepare_client(@namenode_host, @namenode_port, @username)
    if @standby_namenode_host
      @client_standby = prepare_client(@standby_namenode_host, @standby_namenode_port, @username)
    else
      @client_standby = nil
    end

    if not @append
      if @path.index(CHUNK_ID_PLACE_HOLDER).nil?
        raise Fluent::ConfigError, "path must contain ${chunk_id}, which is the placeholder for chunk_id, when append is set to false."
      end
    end
  end

  def prepare_client(host, port, username)
    client = WebHDFS::Client.new(host, port, username)
    if @httpfs
      client.httpfs_mode = true
    end
    client.open_timeout = @open_timeout
    client.read_timeout = @read_timeout
    if @retry_known_errors
      client.retry_known_errors = true
      client.retry_interval = @retry_interval if @retry_interval
      client.retry_times = @retry_times if @retry_times
    end

    client
  end

  def namenode_available(client)
    if client
      available = true
      begin
        client.list('/')
      rescue => e
        $log.warn "webhdfs check request failed. (namenode: #{client.host}:#{client.port}, error: #{e.message})"
        available = false
      end
      available
    else
      false
    end
  end

  def start
    super

    if namenode_available(@client)
      $log.info "webhdfs connection confirmed: #{@namenode_host}:#{@namenode_port}"
      return
    end
    if @client_standby && namenode_available(@client_standby)
      $log.info "webhdfs connection confirmed: #{@standby_namenode_host}:#{@standby_namenode_port}"
      return
    end

    unless @ignore_start_check_error
      raise RuntimeError, "webhdfs is not available now."
    end
  end

  def shutdown
    super
  end

  def path_format(chunk_key)
    Time.strptime(chunk_key, @time_slice_format).strftime(@path)
  end

  def is_standby_exception(e)
    e.is_a?(WebHDFS::IOError) && e.message.match(/org\.apache\.hadoop\.ipc\.StandbyException/)
  end

  def namenode_failover
    if @standby_namenode
      @client, @client_standby = @client_standby, @client
      $log.warn "Namenode failovered, now using #{@client.host}:#{@client.port}."
    end
  end

  def chunk_unique_id_to_str(unique_id)
    unique_id.unpack('C*').map{|x| x.to_s(16).rjust(2,'0')}.join('')
  end

  # TODO check conflictions

  def send_data(path, data)
    if @append
      begin
        @client.append(path, data)
      rescue WebHDFS::FileNotFoundError
        @client.create(path, data)
      end
    else
      @client.create(path, data, {'overwrite' => 'true'})
    end
  end

  def write(chunk)
    hdfs_path = if @append
                  path_format(chunk.key)
                else
                  path_format(chunk.key).gsub(CHUNK_ID_PLACE_HOLDER, chunk_unique_id_to_str(chunk.unique_id))
                end

    failovered = false
    begin
      send_data(hdfs_path, chunk.read)
    rescue => e
      $log.warn "failed to communicate hdfs cluster, path: #{hdfs_path}"

      raise e if !@client_standby || failovered

      if is_standby_exception(e) && namenode_available(@client_standby)
        $log.warn "Seems the connected host status is not active (maybe due to failovers). Gonna try another namenode immediately."
        namenode_failover
        failovered = true
        retry
      end
      if ((@error_history.size + 1) >= @failures_before_use_standby) && namenode_available(@client_standby)
        $log.warn "Too many failures. Try to use the standby namenode instead."
        namenode_failover
        failovered = true
        retry
      end
      raise e
    end
    hdfs_path
  end
end
