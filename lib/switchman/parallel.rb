# frozen_string_literal: true

require 'parallel'

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

    class PrefixingIO
      delegate_missing_to :@original_io

      def initialize(prefix, original_io)
        @prefix = prefix
        @original_io = original_io
      end

      def puts(*args)
        args.flatten.each { |arg| @original_io.puts "#{@prefix}: #{arg}" }
      end
    end
  end
end

::Parallel::UndumpableException.prepend(::Switchman::Parallel::UndumpableException)
