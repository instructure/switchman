# frozen_string_literal: true

require "etc"

module Switchman
  class Environment
    def self.cpu_count(nproc_bin = "nproc")
      return Etc.nprocessors if Etc.respond_to?(:nprocessors)

      `#{nproc_bin}`.to_i
    rescue Errno::ENOENT
      # an environment where nproc` isnt available
      0
    end
  end
end
