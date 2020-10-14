module Fluent::Plugin
  class WebHDFSOutput < Output
    class ZstdCompressor < Compressor
      WebHDFSOutput.register_compressor('zst', self)

      def initialize(options = {})
        begin
          require "zstandard"
        rescue LoadError
          raise Fluent::ConfigError, "Install libzstd before use zstd compressor"
        end
      end

      def ext
        ".zstd"
      end

      def compress(chunk, tmp)
        tmp.binmode
        tmp.write Zstandard.deflate(chunk.read)
      end
    end
  end
end
