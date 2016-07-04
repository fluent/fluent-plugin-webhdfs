module Fluent
  class WebHDFSOutput < Fluent::TimeSlicedOutput
    class SnappyCompressor < Compressor
      WebHDFSOutput.register_compressor('snappy', self)

      def initialize(options = {})
        begin
          require "snappy"
        rescue LoadError
          $log.error("Install snappy before use snappy compressor")
        end
      end

      def ext
        ".sz"
      end

      def compress(chunk, tmp)
        w = Snappy::Writer.new(tmp)
        chunk.write_to(w)
        w.close
      end
    end
  end
end
