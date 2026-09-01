# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module PendingMigrationConnection
      module ClassMethods
        delegate :current_role, :current_switchman_shard, to: :"::ActiveRecord::Base"
      end
    end
  end
end
