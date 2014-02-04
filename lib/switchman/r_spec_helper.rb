require_dependency "switchman/test_helper"

module Switchman
  # including this module in your specs will give you several shards to
  # work with during specs:
  #  * Shard.default - the test database itself
  #  * @shard1 - a shard possibly using the same connection as Shard.default
  #  * @shard2 - a shard using a dedicated connection
  #  * @shard3 - a shard using the same connection as @shard1 (this might
  #              be Shard.default if they already share a connection, or
  #              a separate shard)
  module RSpecHelper
    @@keep_the_shards = false
    @@shard1 = nil

    def self.included(klass)
      # our before handlers have already been configured from a parent group; don't add them again
      return if klass.parent_groups[1..-1].any? { |group| group.included_modules.include?(self) }

      ::RSpec.configure do |config|
        block = proc do
          if @@shard1
            # some specs are mean, and blow away our shards
            begin
              @@shard1.reload
              @@shard2.reload
              @@shard3.reload if @@shard3
            rescue ::ActiveRecord::RecordNotFound
              Shard.default.clone.save!
              Shard.default(true)
              @@shard1 = @@shard1.clone
              @@shard1.save!
              @@shard2 = @@shard2.clone
              @@shard2.save!
              if @@shard3
                @@shard3 = @@shard3.clone
                @@shard3.save!
              end
            end
          end
        end
        # this module will be included multiple times, but we don't want to run this global hook multiple times
        if config.hooks[:before][:all].all? { |hook| hook.block.source_location != block.source_location }
          config.before(:all, &block)
        end
      end

      klass.before(:all) do
        unless @@shard1
          puts "Setting up sharding for all specs..."
          @@shard1, @@shard2 = TestHelper.recreate_persistent_test_shards
          if @@shard1.is_a?(Shard)
            @@keep_the_shards = true
            @@shard3 = nil
          else # @@shard1.is_a?(DatabaseServer)
            begin
              @@shard1 = @@shard1.create_new_shard
              @@shard2 = @@shard2.create_new_shard
              if @@shard1.database_server == Shard.default.database_server
                @@shard3 = nil
              else
                @@shard3 = @@shard1.database_server.create_new_shard
              end
            rescue
              @@shard1 = @@shard2 = @@shard3 = nil
              raise
            end
          end
          puts "Done!"

          at_exit do
            # preserve rspec's exit status
            status= $!.is_a?(::SystemExit) ? $!.status : nil
            puts "Tearing down sharding for all specs"
            @@shard1.database_server.destroy unless @@shard1.database_server == Shard.default.database_server
            unless @@keep_the_shards
              @@shard1.drop_database
              @@shard1.destroy
              @@shard2.drop_database
              @@shard2.destroy
              if @@shard3
                @@shard3.drop_database
                @@shard3.destroy
              end
            end
            @@shard2.database_server.destroy
            exit status if status
          end
        end
        @shard1, @shard2 = @@shard1, @@shard2
        @shard3 = @@shard3 ? @@shard3 : Shard.default
      end

      klass.before do
        Shard.clear_cache
        if use_transactional_fixtures
          Shard.default(true)
          @shard1 = Shard.find(@shard1)
          @shard2 = Shard.find(@shard2)
          shards = [@shard2]
          shards << @shard1 unless @shard1.database_server == Shard.default.database_server
          shards.each do |shard|
            shard.activate do
              # this is how AR does it in fixtures.rb
              ::ActiveRecord::Base.connection.increment_open_transactions
              ::ActiveRecord::Base.connection.transaction_joinable = false
              ::ActiveRecord::Base.connection.begin_db_transaction
            end
          end
        end
      end

      klass.after do
        if use_transactional_fixtures
          shards = [@shard2]
          shards << @shard1 unless @shard1.database_server == Shard.default.database_server
          shards.each do |shard|
            shard.activate do
              if ::ActiveRecord::Base.connection.open_transactions != 0
                ::ActiveRecord::Base.connection.rollback_db_transaction
                ::ActiveRecord::Base.connection.decrement_open_transactions
              end
            end
          end
        end
      end
    end
  end
end
