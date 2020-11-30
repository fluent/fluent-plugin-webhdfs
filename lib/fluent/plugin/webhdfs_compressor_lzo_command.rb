module Fluent::Plugin
  class WebHDFSOutput < Output
    class LZOCommandCompressor < Compressor
      WebHDFSOutput.register_compressor('lzo_command', self)

      config_param :command_parameter, :string, default: '-qf1'

      def initialize(options = {})
        super()
        check_command('lzop', 'LZO')
      end

      def ext
        '.lzo'
      end

      def compress(chunk, tmp)
        w = Tempfile.new("chunk-lzo-tmp-")
        w.binmode
        chunk.write_to(w)
        w.close

        # We don't check the return code because we can't recover lzop failure.
        system "lzop #{@command_parameter} -o #{tmp.path} #{w.path}"
      ensure
        w.close rescue nil
        w.unlink rescue nil
      end
    end
  end
end
