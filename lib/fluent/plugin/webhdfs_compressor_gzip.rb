module Fluent::Plugin
  class WebHDFSOutput < Output
    class GzipCompressor < Compressor
      WebHDFSOutput.register_compressor('gzip', self)

      def initialize(options = {})
        require "zlib"
      end

      def ext
        ".gz"
      end

      def compress(chunk, tmp)
        w = Zlib::GzipWriter.new(tmp)
        chunk.write_to(w)
        w.close
      end
    end
  end
end
