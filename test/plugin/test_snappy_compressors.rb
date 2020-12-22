require "helper"
require "fluent/plugin/buf_memory"
begin
  require "snappy"
rescue LoadError
end

class SnappyCompressorsTest < Test::Unit::TestCase
  class Snappy < self

    CONFIG = %[
      host namenode.local
      path /hdfs/path/file.%Y%m%d.log
    ]

    def setup
      omit unless Object.const_defined?(:Snappy)
      Fluent::Test.setup

      @compressors_size = 2
      @compressors = [
        Fluent::Plugin::WebHDFSOutput::SnappyCompressor.new,
        Fluent::Plugin::WebHDFSOutput::HadoopSnappyCompressor.new
      ]
      @readers = [
        ::Snappy::Reader,
        ::Snappy::Hadoop::Reader
      ]
      @exts = [".sz", ".snappy"]
    end

    def create_driver(conf = CONFIG)
      Fluent::Test::Driver::Output.new(Fluent::Plugin::WebHDFSOutput).configure(conf)
    end

    def test_ext
      for i in 0...@compressors_size do
        assert_equal(@exts[i], @compressors[i].ext)
      end
    end

    def test_compress
      d = create_driver
      if d.instance.respond_to?(:buffer)
        buffer = d.instance.buffer
      else
        buffer = d.instance.instance_variable_get(:@buffer)
      end

      if buffer.respond_to?(:generate_chunk)
        chunk = buffer.generate_chunk("test")
        chunk.concat("hello snappy\n" * 32 * 1024, 1)
      else
        chunk = buffer.new_chunk("test")
        chunk << "hello snappy\n" * 32 * 1024
      end

      for i in 0...@compressors_size do
        io = Tempfile.new("snappy-")
        @compressors[i].compress(chunk, io)
        io.open
        chunk_bytesize = chunk.respond_to?(:bytesize) ? chunk.bytesize : chunk.size
        assert(chunk_bytesize > io.read.bytesize)
        io.rewind
        reader = @readers[i].new(io)
        assert_equal(chunk.read, reader.read)
        io.close
      end
    end
  end
end

