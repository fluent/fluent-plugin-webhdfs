module FluentExt; end

module FluentExt::PlainTextFormatterMixin
  #TODO: tests!
  
  # config_param :output_data_type, :string, :default => 'json' # or 'attr:field' or 'attr:field1,field2,field3(...)'

  attr_accessor :output_include_time, :output_include_tag, :output_data_type
  attr_accessor :add_newline, :field_separator
  attr_accessor :remove_prefix, :default_tag
  
  attr_accessor :f_separator

  def configure(conf)
    super

    @output_include_time = Fluent::Config.bool_value(conf['output_include_time'])
    @output_include_time = true if @output_include_time.nil?

    @output_include_tag = Fluent::Config.bool_value(conf['output_include_tag'])
    @output_include_tag = true if @output_include_tag.nil?

    @output_data_type = conf['output_data_type']
    @output_data_type = 'json' if @output_data_type.nil?

    @f_separator = case conf['field_separator']
                   when 'SPACE' then ' '
                   when 'COMMA' then ','
                   else "\t"
                   end
    @add_newline = Fluent::Config.bool_value(conf['add_newline'])
    if @add_newline.nil?
      @add_newline = true
    end

    @remove_prefix = conf['remove_prefix']
    if @remove_prefix
      @removed_prefix_string = @remove_prefix + '.'
      @removed_length = @removed_prefix_string.length
    end
    if @output_include_tag and @remove_prefix and @remove_prefix.length > 0
      @default_tag = conf['default_tag']
      if @default_tag.nil? or @default_tag.length < 1
        raise Fluent::ConfigError, "Missing 'default_tag' with output_include_tag and remove_prefix."
      end
    end

    # default timezone: utc
    if conf['localtime'].nil? and conf['utc'].nil?
      @utc = true
      @localtime = false
    elsif not @localtime and not @utc
      @utc = true
      @localtime = false
    end
    # mix-in default time formatter (or you can overwrite @timef on your own configure)
    @timef = @output_include_time ? Fluent::TimeFormatter.new(@time_format, @localtime) : nil

    @custom_attributes = []
    if @output_data_type == 'json'
      self.instance_eval {
        def stringify_record(record)
          record.to_json
        end
      }
    elsif @output_data_type =~ /^attr:(.*)$/
      @custom_attributes = $1.split(',')
      if @custom_attributes.size > 1
        self.instance_eval {
          def stringify_record(record)
            @custom_attributes.map{|attr| (record[attr] || 'NULL').to_s}.join(@f_separator)
          end
        }
      elsif @custom_attributes.size == 1
        self.instance_eval {
          def stringify_record(record)
            (record[@custom_attributes[0]] || 'NULL').to_s
          end
        }
      else
        raise Fluent::ConfigError, "Invalid attributes specification: '#{@output_data_type}', needs one or more attributes."
      end
    else
      raise Fluent::ConfigError, "Invalid output_data_type: '#{@output_data_type}'. specify 'json' or 'attr:ATTRIBUTE_NAME' or 'attr:ATTR1,ATTR2,...'"
    end

    if @output_include_time and @output_include_tag
      if @add_newline and @remove_prefix
        self.instance_eval {
          def format(tag,time,record)
            if (tag[0, @removed_length] == @removed_prefix_string and tag.length > @removed_length) or
                tag == @remove_prefix
              tag = tag[@removed_length..-1] || @default_tag
            end
            @timef.format(time) + @f_separator + tag + @f_separator + stringify_record(record) + "\n"
          end
        }
      elsif @add_newline
        self.instance_eval {
          def format(tag,time,record)
            @timef.format(time) + @f_separator + tag + @f_separator + stringify_record(record) + "\n"
          end
        }
      elsif @remove_prefix
        self.instance_eval {
          def format(tag,time,record)
            if (tag[0, @removed_length] == @removed_prefix_string and tag.length > @removed_length) or
                tag == @remove_prefix
              tag = tag[@removed_length..-1] || @default_tag
            end
            @timef.format(time) + @f_separator + tag + @f_separator + stringify_record(record)
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record)
            @timef.format(time) + @f_separator + tag + @f_separator + stringify_record(record)
          end
        }
      end
    elsif @output_include_time
      if @add_newline
        self.instance_eval {
          def format(tag,time,record);
            @timef.format(time) + @f_separator + stringify_record(record) + "\n"
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record);
            @timef.format(time) + @f_separator + stringify_record(record)
          end
        }
      end
    elsif @output_include_tag
      if @add_newline and @remove_prefix
        self.instance_eval {
          def format(tag,time,record)
            if (tag[0, @removed_length] == @removed_prefix_string and tag.length > @removed_length) or
                tag == @remove_prefix
              tag = tag[@removed_length..-1] || @default_tag
            end
            tag + @f_separator + stringify_record(record) + "\n"
          end
        }
      elsif @add_newline
        self.instance_eval {
          def format(tag,time,record)
            tag + @f_separator + stringify_record(record) + "\n"
          end
        }
      elsif @remove_prefix
        self.instance_eval {
          def format(tag,time,record)
            if (tag[0, @removed_length] == @removed_prefix_string and tag.length > @removed_length) or
                tag == @remove_prefix
              tag = tag[@removed_length..-1] || @default_tag
            end
            tag + @f_separator + stringify_record(record)
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record)
            tag + @f_separator + stringify_record(record)
          end
        }
      end
    else # without time, tag
      if @add_newline
        self.instance_eval {
          def format(tag,time,record);
            stringify_record(record) + "\n"
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record);
            stringify_record(record)
          end
        }
      end
    end
  end

  def stringify_record(record)
    record.to_json
  end

  def format(tag, time, record)
    if tag == @remove_prefix or (tag[0, @removed_length] == @removed_prefix_string and tag.length > @removed_length)
      tag = tag[@removed_length..-1] || @default_tag
    end
    time_str = if @output_include_time
                 @timef.format(time) + @f_separator
               else
                 ''
               end
    tag_str = if @output_include_tag
                tag + @f_separator
              else
                ''
              end
    time_str + tag_str + stringify_record(record) + "\n"
  end

end
