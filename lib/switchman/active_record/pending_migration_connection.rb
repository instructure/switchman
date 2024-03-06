# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module PendingMigrationConnection
      def self.current_switchman_shard
        ::ActiveRecord::Base.current_switchman_shard
      end
    end
  end
end
