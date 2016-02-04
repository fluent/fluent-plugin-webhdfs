require "helper"
require "fluent/plugin/buf_memory"
require "snappy"

class CompressorTest < Test::Unit::TestCase
  class Snappy < self
    def setup
      @compressor = Fluent::WebHDFSOutput::SnappyCompressor.new
    end

    def test_ext
      assert_equal(".sz", @compressor.ext)
    end

    def test_compress
      chunk = Fluent::MemoryBufferChunk.new("test")
      chunk << "hello snappy\n" * 32 * 1024
      io = Tempfile.new
      @compressor.compress(chunk, io)
      io.open
      assert(chunk.size > io.read.bytesize)
      io.rewind
      reader = ::Snappy::Reader.new(io)
      assert_equal(chunk.read, reader.read)
      io.close
    end
  end
end

