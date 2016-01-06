module Fluent
  class WebHDFSOutput < Fluent::TimeSlicedOutput
    class TextCompressor < Compressor
      WebHDFSOutput.register_compressor('text', self)

      def ext
        ""
      end

      def compress(chunk, tmp)
        chunk.write_to(tmp)
      end
    end
  end
end
