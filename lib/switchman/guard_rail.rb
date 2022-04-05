# frozen_string_literal: true

module Switchman
  module GuardRail
    module ClassMethods
      def environment
        # no overrides so we get the global role, not the role for the default shard
        ::ActiveRecord::Base.current_role(without_overrides: true)
      end

      def activate(role)
        DatabaseServer.send(:reference_role, role)
        super
      end

      def activate!(role)
        DatabaseServer.send(:reference_role, role)
        super
      end
    end
  end
end
