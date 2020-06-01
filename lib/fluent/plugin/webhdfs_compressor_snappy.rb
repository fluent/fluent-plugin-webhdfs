module Fluent::Plugin
  class WebHDFSOutput < Output
    class SnappyCompressor < Compressor
      WebHDFSOutput.register_compressor('snappy', self)

      def initialize(options = {})
        begin
          require "snappy"
        rescue LoadError
          raise Fluent::ConfigError, "Install snappy before use snappy compressor"
        end
      end

      def ext
        ".sz"
      end

      def compress(chunk, tmp)
        Snappy::Writer.new(tmp) do |w|
          w << chunk.read
          w.flush
        end
      end
    end
  end
end
