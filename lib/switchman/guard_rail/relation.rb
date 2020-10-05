module Switchman
  module GuardRail
    module Relation
      def exec_queries(*args)
        if self.lock_value
          db = Shard.current(shard_category).database_server
          if ::GuardRail.environment != db.guard_rail_environment
            return db.unguard { super }
          end
        end
        super
      end

      %w{update_all delete_all}.each do |method|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{method}(*args)
            db = Shard.current(shard_category).database_server
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
