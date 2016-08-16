# -*- coding: utf-8 -*-

require 'net/http'
require 'time'
require 'webhdfs'
require 'tempfile'
require 'fluent/config/element'
require 'fluent/plugin/output'

require 'fluent/mixin/config_placeholders'
require 'fluent/mixin/plaintextformatter'

class Fluent::Plugin::WebHDFSOutput < Fluent::Plugin::Output
  Fluent::Plugin.register_output('webhdfs', self)

  helpers :compat_parameters

  desc 'WebHDFS/HttpFs host'
  config_param :host, :string, default: nil
  desc 'WebHDFS/HttpFs port'
  config_param :port, :integer, default: 50070
  desc 'Namenode (host:port)'
  config_param :namenode, :string, default: nil # host:port
  desc 'Standby namenode for Namenode HA (host:port)'
  config_param :standby_namenode, :string, default: nil # host:port

  desc 'Ignore errors on start up'
  config_param :ignore_start_check_error, :bool, default: false

  include Fluent::Mixin::ConfigPlaceholders

  desc 'Output file path on HDFS'
  config_param :path, :string
  desc 'User name for pseudo authentication'
  config_param :username, :string, default: nil

  desc 'Store data over HttpFs instead of WebHDFS'
  config_param :httpfs, :bool, default: false

  desc 'Number of seconds to wait for the connection to open'
  config_param :open_timeout, :integer, default: 30 # from ruby net/http default
  desc 'Number of seconds to wait for one block to be read'
  config_param :read_timeout, :integer, default: 60 # from ruby net/http default

  desc 'Retry automatically when known errors of HDFS are occurred'
  config_param :retry_known_errors, :bool, default: false
  desc 'Retry interval'
  config_param :retry_interval, :integer, default: nil
  desc 'The number of retries'
  config_param :retry_times, :integer, default: nil

  # how many times of write failure before switch to standby namenode
  # by default it's 11 times that costs 1023 seconds inside fluentd,
  # which is considered enough to exclude the scenes that caused by temporary network fail or single datanode fail
  desc 'How many times of write failure before switch to standby namenode'
  config_param :failures_before_use_standby, :integer, default: 11

  include Fluent::Mixin::PlainTextFormatter

  config_param :default_tag, :string, default: 'tag_missing'

  desc 'Append data or not'
  config_param :append, :bool, default: true

  desc 'Use SSL or not'
  config_param :ssl, :bool, default: false
  desc 'OpenSSL certificate authority file'
  config_param :ssl_ca_file, :string, default: nil
  desc 'OpenSSL verify mode (none,peer)'
  config_param :ssl_verify_mode, default: nil do |val|
    case val
    when 'none'
      :none
    when 'peer'
      :peer
    else
      raise Fluent::ConfigError, "unexpected parameter on ssl_verify_mode: #{val}"
    end
  end

  desc 'Use kerberos authentication or not'
  config_param :kerberos, :bool, default: false

  SUPPORTED_COMPRESS = ['gzip', 'bzip2', 'snappy', 'lzo_command', 'text']
  desc "Compress method (#{SUPPORTED_COMPRESS.join(',')})"
  config_param :compress, default: nil do |val|
    unless SUPPORTED_COMPRESS.include? val
      raise Fluent::ConfigError, "unsupported compress: #{val}"
    end
    val
  end

  CHUNK_ID_PLACE_HOLDER = '${chunk_id}'

  attr_reader :compressor

  def initialize
    super
    @compressor = nil
  end

  def configure(conf)
    compat_parameters_convert(conf, :buffer, default_chunk_key: "time")

    timekey = case conf["path"]
              when /%S/ then 1
              when /%M/ then 60
              when /%H/ then 3600
              else 86400
              end
    if conf.elements(name: "buffer", arg: "time").empty?
      e = Fluent::Config::Element.new("buffer", "time", {}, [])
      conf.elements << e
    end
    buffer_config = conf.elements(name: "buffer", arg: "time").first
    buffer_config["timekey"] = timekey

    super

    begin
      @compressor = COMPRESSOR_REGISTRY.lookup(@compress || 'text').new
    rescue Fluent::ConfigError
      raise
    rescue
      $log.warn "#{@comress} not found. Use 'text' instead"
      @compressor = COMPRESSOR_REGISTRY.lookup('text').new
    end

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

    unless @append
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
    if @ssl
      client.ssl = true
      client.ssl_ca_file = @ssl_ca_file if @ssl_ca_file
      client.ssl_verify_mode = @ssl_verify_mode if @ssl_verify_mode
    end
    if @kerberos
      client.kerberos = true
    end

    client
  end

  def namenode_available(client)
    if client
      available = true
      begin
        client.list('/')
      rescue => e
        log.warn "webhdfs check request failed. (namenode: #{client.host}:#{client.port}, error: #{e.message})"
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
      log.info "webhdfs connection confirmed: #{@namenode_host}:#{@namenode_port}"
      return
    end
    if @client_standby && namenode_available(@client_standby)
      log.info "webhdfs connection confirmed: #{@standby_namenode_host}:#{@standby_namenode_port}"
      return
    end

    unless @ignore_start_check_error
      raise RuntimeError, "webhdfs is not available now."
    end
  end

  def path_format(metadata)
    extract_placeholders(@path, metadata)
  end

  def is_standby_exception(e)
    e.is_a?(WebHDFS::IOError) && e.message.match(/org\.apache\.hadoop\.ipc\.StandbyException/)
  end

  def namenode_failover
    if @standby_namenode
      @client, @client_standby = @client_standby, @client
      log.warn "Namenode failovered, now using #{@client.host}:#{@client.port}."
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

  def generate_path(chunk)
    hdfs_path = if @append
                  path_format(chunk.metadata)
                else
                  path_format(chunk.metadata).gsub(CHUNK_ID_PLACE_HOLDER, chunk_unique_id_to_str(chunk.unique_id))
                end
    hdfs_path = "#{hdfs_path}#{@compressor.ext}"
    hdfs_path
  end

  def compress_context(chunk, &block)
    begin
      tmp = Tempfile.new("webhdfs-")
      @compressor.compress(chunk, tmp)
      tmp.rewind
      yield tmp
    ensure
      tmp.close(true) rescue nil
    end
  end

  def write(chunk)
    hdfs_path = generate_path(chunk)

    failovered = false
    begin
      compress_context(chunk) do |data|
        send_data(hdfs_path, data)
      end
    rescue => e
      log.warn "failed to communicate hdfs cluster, path: #{hdfs_path}"

      raise e if !@client_standby || failovered

      if is_standby_exception(e) && namenode_available(@client_standby)
        log.warn "Seems the connected host status is not active (maybe due to failovers). Gonna try another namenode immediately."
        namenode_failover
        failovered = true
        retry
      end
      if @num_errors && ((@num_errors + 1) >= @failures_before_use_standby) && namenode_available(@client_standby)
        log.warn "Too many failures. Try to use the standby namenode instead."
        namenode_failover
        failovered = true
        retry
      end
      raise e
    end
    hdfs_path
  end

  class Compressor
    include Fluent::Configurable

    def initialise(options = {})
      super()
    end

    def configure(conf)
      super
    end

    def ext
    end

    def compress(chunk)
    end

    private

    def check_command(command, algo = nil)
      require 'open3'

      algo = command if algo.nil?
      begin
        Open3.capture3("#{command} -V")
      rescue Errno::ENOENT
        raise ConfigError, "'#{command}' utility must be in PATH for #{algo} compression"
      end
    end
  end

  COMPRESSOR_REGISTRY = Fluent::Registry.new(:webhdfs_compressor_type, 'fluent/plugin/webhdfs_compressor_')

  def self.register_compressor(name, compressor)
    COMPRESSOR_REGISTRY.register(name, compressor)
  end
end

require 'fluent/plugin/webhdfs_compressor_text'
require 'fluent/plugin/webhdfs_compressor_gzip'
require 'fluent/plugin/webhdfs_compressor_bzip2'
require 'fluent/plugin/webhdfs_compressor_snappy'
require 'fluent/plugin/webhdfs_compressor_lzo_command'
