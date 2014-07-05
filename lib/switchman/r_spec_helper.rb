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

      klass.before(:all) do
        unless @@shard1
          puts "Setting up sharding for all specs..."
          Shard.delete_all

          @@shard1, @@shard2 = TestHelper.recreate_persistent_test_shards
          @@default_shard = Shard.default
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
        else
          dup = @@default_shard.dup
          dup.id = @@default_shard.id
          dup.save!
          Shard.default(true)
          dup = @@shard1.dup
          dup.id = @@shard1.id
          dup.save!
          @@shard1.instance_variable_set(:@destroyed, false)
          dup = @@shard2.dup
          dup.id = @@shard2.id
          dup.save!
          @@shard2.instance_variable_set(:@destroyed, false)
          if @@shard3
            dup = @@shard3.dup
            dup.id = @@shard3.id
            dup.save!
            @@shard3.instance_variable_set(:@destroyed, false)
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
              # this is how AR does it in database_statements.rb
              if ::Rails.version < '4'
                conn = ::ActiveRecord::Base.connection
                # support nested transactions around (groups of) specs (e.g. for once-ler) 
                if conn.open_transactions == 0
                  conn.transaction_joinable = false
                  conn.begin_db_transaction
                else
                  conn.create_savepoint
                end
                conn.increment_open_transactions
              else
                ::ActiveRecord::Base.connection.begin_transaction joinable: false
              end
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
              if ::Rails.version < '4'
                conn = ::ActiveRecord::Base.connection
                conn.decrement_open_transactions
                if conn.open_transactions == 0
                  conn.rollback_db_transaction
                else
                  conn.rollback_to_savepoint
                end
              else
                ::ActiveRecord::Base.connection.rollback_transaction if ::ActiveRecord::Base.connection.transaction_open?
              end
            end
          end
        end
      end

      klass.after(:all) do
        Shard.default.destroy
        @@shard1.destroy
        @@shard2.destroy
        @@shard3.destroy if @@shard3
        Shard.default(true)
      end
    end
  end
end
