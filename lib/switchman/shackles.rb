module Switchman
  module Shackles
    module ClassMethods
      def ensure_handler
        Shard.default.activate(*Shard.categories) do
          new_handler = @connection_handlers[self.environment]
          if !new_handler
            new_handler = @connection_handlers[self.environment] = ::ActiveRecord::ConnectionAdapters::ConnectionHandler.new
            pools = ::ActiveRecord::Base.connection_handler.instance_variable_get(:@class_to_pool)
            pools.each do |model, pool|
              # don't call establish_connection for pools created just for different sharding categories
              if model != ::ActiveRecord::Base.name
                klass = model.constantize
                next if ::ActiveRecord::Base.connection_handler.retrieve_connection_pool(klass.superclass).default_pool == pools[model].default_pool
              end

              new_handler.establish_connection(model, pool.spec)
            end
          end
          # make sure it picks up the environment change
          new_handler.connection_pools.each do |_, pool|
            pool.spec.instance_variable_set(:@current_config, nil)
          end
          new_handler
        end
      end
    end

    def self.included(klass)
      klass.extend(ClassMethods)
      klass.singleton_class.send(:remove_method, :ensure_handler)
    end
  end
end
