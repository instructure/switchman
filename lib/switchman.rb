require "shackles"
require "switchman/engine"

module Switchman
  def self.config
    # TODO: load from yaml
    @config ||= {}
  end
end
