require "helper"
require "fluent/plugin/buf_memory"
begin
  require "zlib"
rescue LoadError
end

class GzipCompressorTest < Test::Unit::TestCase
  class Gzip < self

    CONFIG = %[
      host namenode.local
      path /hdfs/path/file.%Y%m%d.log
    ]

    def setup
      omit unless Object.const_defined?(:Zlib)
      Fluent::Test.setup
      @compressor = Fluent::Plugin::WebHDFSOutput::GzipCompressor.new
    end

    def create_driver(conf = CONFIG)
      Fluent::Test::Driver::Output.new(Fluent::Plugin::WebHDFSOutput).configure(conf)
    end

    def test_ext
      assert_equal(".gz", @compressor.ext)
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
        chunk.concat("hello gzip\n" * 32 * 1024, 1)
      else
        chunk = buffer.new_chunk("test")
        chunk << "hello gzip\n" * 32 * 1024
      end

      io = Tempfile.new("gzip-")
      @compressor.compress(chunk, io)
      assert !io.closed?
      chunk_bytesize = chunk.respond_to?(:bytesize) ? chunk.bytesize : chunk.size
      assert(chunk_bytesize > io.read.bytesize)
      io.rewind
      reader = Zlib::GzipReader.new(io)
      assert_equal(chunk.read, reader.read)
      io.close
    end
  end
end
