# frozen_string_literal: true

require 'etc'

module Switchman
  class Environment

    def self.cpu_count(nproc_bin = "nproc")
      if Etc.respond_to?(:nprocessors)
        return Etc.nprocessors
      end

      return `#{nproc_bin}`.to_i
    rescue Errno::ENOENT
      # an environment where nproc` isnt available
      return 0
    end

  end
end
