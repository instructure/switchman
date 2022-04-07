# frozen_string_literal: true

require 'guard_rail'
require 'zeitwerk'

class SwitchmanInflector < Zeitwerk::GemInflector
  def camelize(basename, abspath)
    if basename =~ /\Apostgresql_(.*)/
      'PostgreSQL' + super($1, abspath)
    else
      super
    end
  end
end

loader = Zeitwerk::Loader.for_gem
loader.inflector = SwitchmanInflector.new(__FILE__)
loader.setup

module Switchman
  def self.config
    # TODO: load from yaml
    @config ||= {}
  end

  def self.cache
    (@cache.respond_to?(:call) ? @cache.call : @cache) || ::Rails.cache
  end

  def self.cache=(cache)
    @cache = cache
  end

  def self.foreign_key_check(name, type, limit: nil)
    puts "WARNING: All foreign keys need to be 8-byte integers. #{name} looks like a foreign key. If so, please add the option: `:limit => 8`" if name.to_s =~ /_id\z/ && type.to_s == 'integer' && limit.to_i < 8
  end

  class OrderOnMultiShardQuery < RuntimeError; end
end

# Load the engine and everything associated at gem load time
Switchman::Engine
