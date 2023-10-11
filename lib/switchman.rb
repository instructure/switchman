# frozen_string_literal: true

require "guard_rail"
require "zeitwerk"

class SwitchmanInflector < Zeitwerk::GemInflector
  def camelize(basename, abspath)
    if basename =~ /\Apostgresql_(.*)/
      "PostgreSQL" + super($1, abspath)
    else
      super
    end
  end
end

loader = Zeitwerk::Loader.for_gem
loader.inflector = SwitchmanInflector.new(__FILE__)
loader.setup

module Switchman
  Deprecation = ::ActiveSupport::Deprecation.new("4.0", "Switchman")

  class << self
    attr_writer :cache

    def config
      # TODO: load from yaml
      @config ||= {}
    end

    def cache
      (@cache.respond_to?(:call) ? @cache.call : @cache) || ::Rails.cache
    end

    def region
      config[:region]
    end

    def foreign_key_check(name, type, limit: nil)
      return unless name.to_s.end_with?("_id") && type.to_s == "integer" && limit.to_i < 8

      puts <<~TEXT.squish
        WARNING: All foreign keys need to be 8-byte integers.
        #{name} looks like a foreign key.
        If so, please add the option: `:limit => 8`
      TEXT
    end
  end

  class OrderOnMultiShardQuery < RuntimeError; end
end

# Load the engine and everything associated at gem load time
Switchman::Engine
