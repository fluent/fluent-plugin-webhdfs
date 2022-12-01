# -*- coding: utf-8 -*-

require 'fluent/plugin/output'
require 'fluent/config/element'

require 'webhdfs'
require 'tempfile'
require 'securerandom'

class Fluent::Plugin::WebHDFSOutput < Fluent::Plugin::Output
  Fluent::Plugin.register_output('webhdfs', self)

  helpers :inject, :formatter, :compat_parameters

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

  config_param :end_with_newline, :bool, default: true

  desc 'Append data or not'
  config_param :append, :bool, default: true

  desc 'Use SSL or not'
  config_param :ssl, :bool, default: false
  desc 'OpenSSL certificate authority file'
  config_param :ssl_ca_file, :string, default: nil
  desc 'OpenSSL verify mode (none,peer)'
  config_param :ssl_verify_mode, :enum, list: [:none, :peer], default: :none

  desc 'Use kerberos authentication or not'
  config_param :kerberos, :bool, default: false
  desc 'kerberos keytab file'
  config_param :kerberos_keytab, :string, default: nil
  desc 'Use delegation token while upload webhdfs or not'
  config_param :renew_kerberos_delegation_token, :bool, default: false
  desc 'delegation token reuse timer (default 8h)'
  config_param :renew_kerberos_delegation_token_interval, :time, default: 8 * 60 * 60

  SUPPORTED_COMPRESS = [:gzip, :bzip2, :snappy, :hadoop_snappy, :lzo_command, :zstd, :text]
  desc "Compression method (#{SUPPORTED_COMPRESS.join(',')})"
  config_param :compress, :enum, list: SUPPORTED_COMPRESS, default: :text

  desc 'HDFS file extensions (overrides default compressor extensions)'
  config_param :extension, :string, default: nil

  config_param :remove_prefix, :string, default: nil, deprecated: "use @label for routing"
  config_param :default_tag, :string, default: nil, deprecated: "use @label for routing"
  config_param :null_value, :string, default: nil, deprecated: "use filter plugins to convert null values into any specified string"
  config_param :suppress_log_broken_string, :bool, default: false, deprecated: "use @log_level for plugin to suppress such info logs"

  CHUNK_ID_PLACE_HOLDER = '${chunk_id}'

  config_section :buffer do
    config_set_default :chunk_keys, ["time"]
  end

  config_section :format do
    config_set_default :@type, 'out_file'
    config_set_default :localtime, false # default timezone is UTC
  end

  attr_reader :formatter, :compressor

  def initialize
    super
    @compressor = nil
    @standby_namenode_host = nil
    @output_include_tag = @output_include_time = nil # TODO: deprecated
    @header_separator = @field_separator = nil # TODO: deprecated
  end

  def configure(conf)
    # #compat_parameters_convert ignore time format in conf["path"],
    # so check conf["path"] and overwrite the default value later if needed
    timekey = case conf["path"]
              when /%S/ then 1
              when /%M/ then 60
              when /%H/ then 3600
              else 86400
              end
    if buffer_config = conf.elements(name: "buffer").first
      timekey = buffer_config["timekey"] || timekey 
    end

    compat_parameters_convert(conf, :buffer, default_chunk_key: "time")

    if conf.elements(name: "buffer").empty?
      e = Fluent::Config::Element.new("buffer", "time", {}, [])
      conf.elements << e
    end
    buffer_config = conf.elements(name: "buffer").first
    # explicitly set timekey
    buffer_config["timekey"] = timekey

    compat_parameters_convert_plaintextformatter(conf)
    verify_config_placeholders_in_path!(conf)

    super

    @formatter = formatter_create

    if @using_formatter_config
      @null_value = nil
    else
      @formatter.delimiter = "\x01" if @formatter.respond_to?(:delimiter) && @formatter.delimiter == 'SOH'
      @null_value ||= 'NULL'
    end

    if @default_tag.nil? && !@using_formatter_config && @output_include_tag
      @default_tag = "tag_missing"
    end
    if @remove_prefix
      @remove_prefix_actual = @remove_prefix + "."
      @remove_prefix_actual_length = @remove_prefix_actual.length
    end

    @replace_random_uuid = @path.include?('%{uuid}') || @path.include?('%{uuid_flush}')
    if @replace_random_uuid
      # to check SecureRandom.uuid is available or not (NotImplementedError raised in such environment)
      begin
        SecureRandom.uuid
      rescue
        raise Fluent::ConfigError, "uuid feature (SecureRandom) is unavailable in this environment"
      end
    end

    @compressor = COMPRESSOR_REGISTRY.lookup(@compress.to_s).new
    @compressor.configure(conf)

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

    # If you're running three or more name nodes
    # I want to name "standby_namenode" to "standby_namenodes", but I keep it's name because there may be compatibility issues
    if @standby_namenode
      # we use standby_namenode_group for switch namenode freely
      @standby_namenode_group = Array.new
      for standby_namenode_content in @standby_namenode.split do
        unless /\A([a-zA-Z0-9][-a-zA-Z0-9.]*):(\d+)\Z/ =~ standby_namenode_content
          raise Fluent::ConfigError, "Invalid config value about standby namenode: '#{standby_namenode_content}', needs STANDBY_NAMENODE_HOST:PORT"
        end
        if @httpfs
          raise Fluent::ConfigError, "Invalid configuration: specified to use both of standby_namenode and httpfs."
        end
        @standby_namenode_group << {host:$1 ,port:$2.to_i}
      end
    end

    unless @path.index('/') == 0
      raise Fluent::ConfigError, "Path on hdfs MUST starts with '/', but '#{@path}'"
    end

    @renew_kerberos_delegation_token_interval_hour = nil
    if @renew_kerberos_delegation_token
      unless @username
        raise Fluent::ConfigError, "username is missing. If you want to reuse delegation token, follow with kerberos accounts"
      end
      @renew_kerberos_delegation_token_interval_hour = @renew_kerberos_delegation_token_interval / 60 / 60
    end
    
    @client = prepare_client(@namenode_host, @namenode_port, @username)
    # Use clients_standby for finding available namenode
    if @standby_namenode_group
      @clients_standby = @standby_namenode_group.map{ |node| prepare_client(node[:host],node[:port],@username) }
      @clients_standby = @clients_standby.prepend(@client)
    else
      @clients_standby = nil
    end

    unless @append
      if @path.index(CHUNK_ID_PLACE_HOLDER).nil?
        raise Fluent::ConfigError, "path must contain ${chunk_id}, which is the placeholder for chunk_id, when append is set to false."
      end
    end
  end

  def multi_workers_ready?
    true
  end

  def prepare_client(host, port, username)
    client = WebHDFS::Client.new(host, port, username, nil, nil, nil, {}, @renew_kerberos_delegation_token_interval_hour)
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
      client.kerberos_keytab = @kerberos_keytab if @kerberos_keytab
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
    # if we use "standby_namenode", check all of available standby_namenodes
    if @clients_standby
      for client_standby_check in @clients_standby do
        if namenode_available(client_standby_check)
          log.info "webhdfs connection confirmed: #{client_standby_check.host}:#{client_standby_check.port}"
          return
        end
      end
    end

    unless @ignore_start_check_error
      raise RuntimeError, "webhdfs is not available now."
    end
  end

  def is_standby_exception(e)
    e.is_a?(WebHDFS::IOError) && e.message.match(/org\.apache\.hadoop\.ipc\.StandbyException/)
  end

  # Add function
  # Change the main namenode to the value of a specific index in the standby namenode.
  def namenode_replace_to_standby(index)
    @client_ha_index = index
    @client = @clients_standby[@client_ha_index]
    log.warn "Namenode failovered, now using #{@client.host}:#{@client.port}."
  end

  def send_data(path, data)
    return @client.create(path, data, {'overwrite' => 'true'}) unless @append

    if path_exists?(path)
      @client.append(path, data)
    else
      @client.create(path, data)
    end
  end

  def path_exists?(path)
    @client.stat(path)
    true
  rescue WebHDFS::FileNotFoundError
    false
  end

  HOSTNAME_PLACEHOLDERS_DEPRECATED = ['${hostname}', '%{hostname}', '__HOSTNAME__']
  UUID_RANDOM_PLACEHOLDERS_DEPRECATED = ['${uuid}', '${uuid:random}', '__UUID__', '__UUID_RANDOM__']
  UUID_OTHER_PLACEHOLDERS_OBSOLETED = ['${uuid:hostname}', '%{uuid:hostname}', '__UUID_HOSTNAME__', '${uuid:timestamp}', '%{uuid:timestamp}', '__UUID_TIMESTAMP__']

  def verify_config_placeholders_in_path!(conf)
    return unless conf.has_key?('path')

    path = conf['path']

    # check @path for ${hostname}, %{hostname} and __HOSTNAME__ to warn to use #{Socket.gethostbyname}
    if HOSTNAME_PLACEHOLDERS_DEPRECATED.any?{|ph| path.include?(ph) }
      log.warn "hostname placeholder is now deprecated. use '\#\{Socket.gethostname\}' instead."
      hostname = conf['hostname'] || Socket.gethostname
      HOSTNAME_PLACEHOLDERS_DEPRECATED.each do |ph|
        path.gsub!(ph, hostname)
      end
    end

    if UUID_RANDOM_PLACEHOLDERS_DEPRECATED.any?{|ph| path.include?(ph) }
      log.warn "random uuid placeholders are now deprecated. use %{uuid} (or %{uuid_flush}) instead."
      UUID_RANDOM_PLACEHOLDERS_DEPRECATED.each do |ph|
        path.gsub!(ph, '%{uuid}')
      end
    end

    if UUID_OTHER_PLACEHOLDERS_OBSOLETED.any?{|ph| path.include?(ph) }
      UUID_OTHER_PLACEHOLDERS_OBSOLETED.each do |ph|
        if path.include?(ph)
          log.error "configuration placeholder #{ph} is now unsupported by webhdfs output plugin."
        end
      end
      raise ConfigError, "there are unsupported placeholders in path."
    end
  end

  def generate_path(chunk)
    hdfs_path = if @append
                  extract_placeholders(@path, chunk)
                else
                  extract_placeholders(@path.gsub(CHUNK_ID_PLACE_HOLDER, dump_unique_id_hex(chunk.unique_id)), chunk)
                end
    hdfs_ext = @extension || @compressor.ext
    hdfs_path = "#{hdfs_path}#{hdfs_ext}"
    if @replace_random_uuid
      uuid_random = SecureRandom.uuid
      hdfs_path = hdfs_path.gsub('%{uuid}', uuid_random).gsub('%{uuid_flush}', uuid_random)
    end
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

  def format(tag, time, record)
    if @remove_prefix # TODO: remove when it's obsoleted
      if tag.start_with?(@remove_prefix_actual)
        if tag.length > @remove_prefix_actual_length
          tag = tag[@remove_prefix_actual_length..-1]
        else
          tag = @default_tag
        end
      elsif tag.start_with?(@remove_prefix)
        if tag == @remove_prefix
          tag = @default_tag
        else
          tag = tag.sub(@remove_prefix, '')
        end
      end
    end

    if @null_value # TODO: remove when it's obsoleted
      check_keys = (record.keys + @null_convert_keys).uniq
      check_keys.each do |key|
        record[key] = @null_value if record[key].nil?
      end
    end

    if @using_formatter_config
      record = inject_values_to_record(tag, time, record)
      line = @formatter.format(tag, time, record)
    else # TODO: remove when it's obsoleted
      time_str = @output_include_time ? @time_formatter.call(time) + @header_separator : ''
      tag_str = @output_include_tag ? tag + @header_separator : ''
      record_str = @formatter.format(tag, time, record)
      line = time_str + tag_str + record_str
    end
    line << "\n" if @end_with_newline && !line.end_with?("\n")
    line
  rescue => e # remove this clause when @suppress_log_broken_string is obsoleted
    unless @suppress_log_broken_string
      log.info "unexpected error while formatting events, ignored", tag: tag, record: record, error: e
    end
    ''
  end

  # Modify function
  # if occured the failover, check standby_namenodes and change available standby namenode to main namenode
  def namenode_failover
    @clients_standby.each_with_index do |client_standby, idx|
      if namenode_available(client_standby)
        namenode_replace_to_standby(idx)
        return true
      end
    end
    return false
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

      raise e if !@clients_standby || failovered

      if is_standby_exception(e) && namenode_failover
        log.warn "Seems the connected host status is not active (maybe due to failovers). Gonna try another namenode immediately."
        failovered = true
        retry
      end
      if @num_errors && ((@num_errors + 1) >= @failures_before_use_standby) && namenode_failover
        log.warn "Too many failures. Try to use the standby namenode instead."
        failovered = true
        retry
      end
      raise e
    end
    hdfs_path
  end

  def compat_parameters_convert_plaintextformatter(conf)
    if !conf.elements('format').empty? || !conf['output_data_type']
      @using_formatter_config = true
      @null_convert_keys = []
      return
    end

    log.warn "webhdfs output plugin is working with old configuration parameters. use <inject>/<format> sections instead for further releases."
    @using_formatter_config = false
    @null_convert_keys = []

    @header_separator = case conf['field_separator']
                        when nil     then "\t"
                        when 'SPACE' then ' '
                        when 'TAB'   then "\t"
                        when 'COMMA' then ','
                        when 'SOH'   then "\x01"
                        else conf['field_separator']
                        end

    format_section = Fluent::Config::Element.new('format', '', {}, [])
    case conf['output_data_type']
    when '', 'json' # blank value is for compatibility reason (especially in testing)
      format_section['@type'] = 'json'
    when 'ltsv'
      format_section['@type'] = 'ltsv'
    else
      unless conf['output_data_type'].start_with?('attr:')
        raise Fluent::ConfigError, "output_data_type is invalid: #{conf['output_data_type']}"
      end
      format_section['@format'] = 'tsv'
      keys_part = conf['output_data_type'].sub(/^attr:/, '')
      @null_convert_keys = keys_part.split(',')
      format_section['keys'] = keys_part
      format_section['delimiter'] = case conf['field_separator']
                                    when nil then '\t'
                                    when 'SPACE' then ' '
                                    when 'TAB'   then '\t'
                                    when 'COMMA' then ','
                                    when 'SOH'   then 'SOH' # fixed later
                                    else conf['field_separator']
                                    end
    end

    conf.elements << format_section

    @output_include_time = conf.has_key?('output_include_time') ? Fluent::Config.bool_value(conf['output_include_time']) : true
    @output_include_tag = conf.has_key?('output_include_tag') ? Fluent::Config.bool_value(conf['output_include_tag']) : true

    if @output_include_time
      # default timezone is UTC
      using_localtime = if !conf.has_key?('utc') && !conf.has_key?('localtime')
                          false
                        elsif conf.has_key?('localtime') && conf.has_key?('utc')
                          raise Fluent::ConfigError, "specify either 'localtime' or 'utc'"
                        elsif conf.has_key?('localtime')
                          Fluent::Config.bool_value('localtime')
                        else
                          Fluent::Config.bool_value('utc')
                        end
      @time_formatter = Fluent::TimeFormatter.new(conf['time_format'], using_localtime)
    else
      @time_formatter = nil
    end
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
        raise Fluent::ConfigError, "'#{command}' utility must be in PATH for #{algo} compression"
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
require 'fluent/plugin/webhdfs_compressor_hadoop_snappy'
require 'fluent/plugin/webhdfs_compressor_lzo_command'
require 'fluent/plugin/webhdfs_compressor_zstd'
