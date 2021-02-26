# frozen_string_literal: true

require 'guard_rail'
require 'switchman/open4'
require 'switchman/engine'

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
end
