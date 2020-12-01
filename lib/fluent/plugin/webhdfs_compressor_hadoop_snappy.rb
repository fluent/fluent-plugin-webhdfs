module Fluent::Plugin
  class WebHDFSOutput < Output
    class HadoopSnappyCompressor < Compressor
      WebHDFSOutput.register_compressor('hadoop_snappy', self)

      DEFAULT_BLOCK_SIZE = 256 * 1024

      desc 'Block size for compression algorithm'
      config_param :block_size, :integer, default: DEFAULT_BLOCK_SIZE

      def initialize(options = {})
        super()
        begin
          require "snappy"
        rescue LoadError
          raise Fluent::ConfigError, "Install snappy before using snappy compressor"
        end
      end

      def ext
        ".snappy"
      end

      def compress(chunk, tmp)
        Snappy::Hadoop::Writer.new(tmp, @block_size) do |w|
          w << chunk.read
          w.flush
        end
      end
    end
  end
end
