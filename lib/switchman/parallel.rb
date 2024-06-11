# frozen_string_literal: true

require "parallel"

module Switchman
  module Parallel
    module UndumpableException
      def initialize(original)
        super
        @active_shards = original.instance_variable_get(:@active_shards)
        current_shard
      end
    end

    class QuietExceptionWrapper
      attr_accessor :name

      def initialize(name, wrapper)
        @name = name
        @wrapper = wrapper
      end

      def exception
        @wrapper.exception
      end
    end

    class UndumpableResult
      attr_reader :name

      def initialize(result)
        @name = result.inspect
      end

      def inspect
        "#<UndumpableResult:#{name}>"
      end
    end

    class ResultWrapper
      attr_reader :result

      def initialize(result)
        @result =
          begin
            Marshal.dump(result) && result
          rescue
            UndumpableResult.new(result)
          end
      end
    end

    class TransformingIO
      delegate_missing_to :@original_io

      def initialize(transformer, original_io)
        @transformer = transformer
        @original_io = original_io
      end

      def puts(*args)
        args.flatten.each { |arg| @original_io.puts @transformer.call(arg) }
      end
    end
  end
end

Parallel::UndumpableException.prepend(Switchman::Parallel::UndumpableException)
