# frozen_string_literal: true

require "switchman/test_helper"

module Switchman
  # including this module in your specs will give you several shards to
  # work with during specs:
  #  * Shard.default - the test database itself
  #  * @shard1 - a shard using the same connection as Shard.default
  #  * @shard2 - a shard using a dedicated connection
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
        next unless groups.any? { |descendant_group| RSpecHelper.included_in?(descendant_group) }

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
          rescue => e
            warn "Sharding setup FAILED!:"
            while e
              warn "\n#{e}\n"
              warn e.backtrace
              e = e.respond_to?(:cause) ? e.cause : nil
            end
            @@sharding_failed = true
            @@shard1&.drop_database rescue nil
            @@shard2&.drop_database rescue nil
            @@shard1 = @@shard2 = nil
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

        main_pid = Process.pid
        at_exit do
          next unless main_pid == Process.pid

          # preserve rspec's exit status
          status = $!.is_a?(::SystemExit) ? $!.status : nil
          puts "Tearing down sharding for all specs"
          @@shard1.database_server.destroy unless @@shard1.database_server == Shard.default.database_server
          unless @@keep_the_shards
            @@shard1.drop_database
            @@shard1.destroy
            @@shard2.drop_database
            @@shard2.destroy
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
        @shard1, @shard2 = @@shard1, @@shard2
      end

      klass.before do
        raise "Sharding did not set up correctly" if @@sharding_failed

        Shard.clear_cache
        if use_transactional_tests
          Shard.default(reload: true)
          @shard1 = Shard.find(@shard1.id)
          @shard2 = Shard.find(@shard2.id)
        end
      end

      klass.after do
        next if @@sharding_failed

        # clean up after specs
        DatabaseServer.each do |ds|
          if ds.fake? && ds != @shard2.database_server
            ds.shards.delete_all unless use_transactional_tests
            ds.destroy
          end
          ds.remove_instance_variable(:@primary_shard_id) if ds.instance_variable_defined?(:@primary_shard_id)
        end
      end

      klass.after(:all) do
        # Don't truncate because that can create some fun cross-connection lock contention
        Shard.delete_all
        Switchman.cache.delete("default_shard")
        Shard.default(reload: true)
      end
    end
  end
end
