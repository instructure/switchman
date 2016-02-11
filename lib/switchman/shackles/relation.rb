module Switchman
  module Shackles
    module Relation
      def exec_queries(*args)
        if self.lock_value
          db = Shard.current(shard_category).database_server
          if ::Shackles.environment != db.shackles_environment
            return db.unshackle { super }
          end
        end
        super
      end

      %w{update_all delete_all}.each do |method|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{method}(*args)
            db = Shard.current(shard_category).database_server
            if ::Shackles.environment != db.shackles_environment
              db.unshackle { super }
            else
              super
            end
          end
        RUBY
      end
    end
  end
end
