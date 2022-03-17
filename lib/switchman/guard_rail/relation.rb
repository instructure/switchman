# frozen_string_literal: true

module Switchman
  module GuardRail
    module Relation
      def exec_queries(*args)
        if lock_value
          db = Shard.current(connection_class_for_self).database_server
          db.unguard { super }
        else
          super
        end
      end

      %w[update_all delete_all].each do |method|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{method}(*args)
            db = Shard.current(connection_class_for_self).database_server
            db.unguard { super }
          end
        RUBY
      end
    end
  end
end
