# frozen_string_literal: true

module Switchman
  module GuardRail
    module ClassMethods
      def activate(role)
        DatabaseServer.reference_role(role)
        super
      end

      def activate!(role)
        DatabaseServer.reference_role(role)
        super
      end
    end
  end
end
