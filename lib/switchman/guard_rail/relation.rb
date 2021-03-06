# frozen_string_literal: true

module Switchman
  module GuardRail
    module Relation
      def exec_queries(*args)
        if lock_value
          db = Shard.current(connection_classes).database_server
          return db.unguard { super } if ::GuardRail.environment != db.guard_rail_environment
        end
        super
      end

      %w[update_all delete_all].each do |method|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{method}(*args)
            db = Shard.current(connection_classes).database_server
            if ::GuardRail.environment != db.guard_rail_environment
              db.unguard { super }
            else
              super
            end
          end
        RUBY
      end
    end
  end
end
