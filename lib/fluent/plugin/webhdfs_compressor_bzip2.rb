module Fluent
  class WebHDFSOutput < Fluent::TimeSlicedOutput
    class Bzip2Compressor < Compressor
      WebHDFSOutput.register_compressor('bzip2', self)

      def initialize(options = {})
        require "bzip2/ffi"
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
