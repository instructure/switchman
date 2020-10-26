# frozen_string_literal: true

require "switchman/test_helper"

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
    @@sharding_failed = false

    def self.included_in?(klass)
      klass.parent_groups.any? { |group| group.included_modules.include?(self) }
    end

    def self.included(klass)
      # our before handlers have already been configured from a parent group; don't add them again
      parent_group = klass.parent_groups[1]
      return if parent_group && included_in?(parent_group)

      # set up sharding schemas/dbs before the root group runs, so that
      # they persist across transactional groups (e.g. once-ler)
      root_group = klass.parent_groups.last
      root_group.prepend_before(:all) do |group|
        next if @@shard1
        next if @@sharding_failed
        # if we aren't actually going to run a sharding group/example,
        # don't set it up after all
        groups = group.class.descendant_filtered_examples.map(&:example_group).uniq
        next unless groups.any?{ |group| RSpecHelper.included_in?(group) }

        puts "Setting up sharding for all specs..."
        Shard.delete_all
        Switchman.cache.delete("default_shard")

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
          rescue => e
            $stderr.puts "Sharding setup FAILED!:"
            while e
              $stderr.puts "\n#{e}\n"
              $stderr.puts e.backtrace
              e = e.respond_to?(:cause) ? e.cause : nil
            end
            @@sharding_failed = true
            (@@shard1.drop_database if @@shard1) rescue nil
            (@@shard2.drop_database if @@shard3) rescue nil
            (@@shard3.drop_database if @@shard3) rescue nil
            @@shard1 = @@shard2 = @@shard3 = nil
            Shard.delete_all
            Shard.default(reload: true)
            next
          end
        end
        # we'll re-persist in the group's `before :all`; we don't want them to exist
        # in the db before then
        Shard.delete_all
        Switchman.cache.delete("default_shard")
        Shard.default(reload: true)
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

      klass.before(:all) do
        next if @@sharding_failed
        dup = @@default_shard.dup
        dup.id = @@default_shard.id
        dup.save!
        Switchman.cache.delete("default_shard")
        Shard.default(reload: true)
        dup = @@shard1.dup
        dup.id = @@shard1.id
        dup.save!
        dup = @@shard2.dup
        dup.id = @@shard2.id
        dup.save!
        if @@shard3
          dup = @@shard3.dup
          dup.id = @@shard3.id
          dup.save!
        end
        @shard1, @shard2 = @@shard1, @@shard2
        @shard3 = @@shard3 ? @@shard3 : Shard.default
      end

      klass.before do
        raise "Sharding did not set up correctly" if @@sharding_failed
        Shard.clear_cache
        if use_transactional_tests
          Shard.default(reload: true)
          @shard1 = Shard.find(@shard1.id)
          @shard2 = Shard.find(@shard2.id)
          shards = [@shard2]
          shards << @shard1 unless @shard1.database_server == Shard.default.database_server
          shards.each do |shard|
            shard.activate do
              ::ActiveRecord::Base.connection.begin_transaction joinable: false
            end
          end
        end
      end

      klass.after do
        next if @@sharding_failed
        if use_transactional_tests
          shards = [@shard2]
          shards << @shard1 unless @shard1.database_server == Shard.default.database_server
          shards.each do |shard|
            shard.activate do
              ::ActiveRecord::Base.connection.rollback_transaction if ::ActiveRecord::Base.connection.transaction_open?
            end
          end
        end
      end

      klass.after(:all) do
        Shard.connection.update("TRUNCATE #{Shard.quoted_table_name} CASCADE")
        Switchman.cache.delete("default_shard")
        Shard.default(reload: true)
      end
    end
  end
end
