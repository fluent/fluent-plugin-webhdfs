module Fluent::Plugin
  class WebHDFSOutput < Output
    class Bzip2Compressor < Compressor
      WebHDFSOutput.register_compressor('bzip2', self)

      def initialize(options = {})
        begin
          require "bzip2/ffi"
        rescue LoadError
          raise Fluent::ConfigError, "Install bzip2-ffi before use bzip2 compressor"
        end
      end

      def ext
        ".bz2"
      end

      def compress(chunk, tmp)
        Bzip2::FFI::Writer.open(tmp) do |writer|
          chunk.write_to(writer)
        end
      end
    end
  end
end
