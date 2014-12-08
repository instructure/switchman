module Switchman
  module Rake
    def self.filter_database_servers(&block)
      chain = filter_database_servers_chain # use a local variable so that the current chain is closed over in the following lambda
      @filter_database_servers_chain = lambda { |servers| block.call(servers, chain) }
    end

    def self.shardify_task(task_name)
      old_task = ::Rake::Task[task_name]
      old_actions = old_task.actions.dup
      old_task.actions.clear

      old_task.enhance do
        if ::Rails.env.test?
          require 'switchman/test_helper'
          TestHelper.recreate_persistent_test_shards(dont_create: true)
        end

        ::Shackles.activate(:deploy) do
          servers = DatabaseServer.all

          if ENV['DATABASE_SERVER']
            servers = ENV['DATABASE_SERVER']
            if servers.first == '-'
              negative = true
              servers = servers[1..-1]
            end
            servers = servers.split(',').map { |server| DatabaseServer.find(server) }.compact
            servers = DatabaseServer.all - servers if negative
          end

          servers = filter_database_servers_chain.call(servers)

          scope = Shard.order("database_server_id IS NOT NULL, database_server_id, id")
          if servers != DatabaseServer.all
            conditions = ["database_server_id IN (?)", servers.map(&:id)]
            conditions.first << " OR database_server_id IS NULL" if servers.include?(Shard.default.database_server)
            scope = scope.where(conditions)
          end

          if ENV['SHARD']
            scope = shard_scope(scope, ENV['SHARD'])
          end

          Shard.with_each_shard(scope, Shard.categories, :parallel => ENV['PARALLEL'].to_i) do
            shard = Shard.current
            puts "#{shard.id}: #{shard.description}"
            ::ActiveRecord::Base.connection_pool.spec.config[:shard_name] = Shard.current.name
            ::ActiveRecord::Base.configurations[::Rails.env] = ::ActiveRecord::Base.connection_pool.spec.config.stringify_keys
            shard.database_server.unshackle do
              old_actions.each(&:call)
            end
            nil
          end
        end
      end
    end

    %w{db:migrate db:migrate:up db:migrate:down db:rollback}.each { |task_name| shardify_task(task_name) }

    private

    def self.shard_scope(scope, raw_shard_ids)
      raw_shard_ids = raw_shard_ids.split(',')

      shard_ids = []
      negative_shard_ids = []
      ranges = []
      negative_ranges = []
      raw_shard_ids.each do |id|
        if id == 'default'
          shard_ids << Shard.default.id
        elsif id == '-default'
          negative_shard_ids << Shard.default.id
        elsif id =~ /(-?)(\d+)?\.\.(\.)?(\d+)?/
          negative, start, open, finish = $1.present?, $2, $3.present?, $4
          raise "Invalid shard id or range: #{id}" unless start || finish
          range = []
          range << "id>=#{start}" if start
          range << "id<#{'=' unless open}#{finish}" if finish
          (negative ? negative_ranges : ranges) << "(#{range.join(' AND ')})"
        elsif id =~ /-(\d+)/
          negative_shard_ids << $1.to_i
        elsif id =~ /\d+/
          shard_ids << id.to_i
        else
          raise "Invalid shard id or range: #{id}"
        end
      end

      shard_ids.uniq!
      negative_shard_ids.uniq!
      unless shard_ids.empty?
        shard_ids -= negative_shard_ids
        if shard_ids.empty? && ranges.empty?
          if ::Rails.version < '4'
            return scope.where("?", false)
          else
            return scope.none
          end
        end
        # we already trimmed them all out; no need to make the server do it as well
        negative_shard_ids = [] if ranges.empty?
      end

      conditions = []
      positive_queries = []
      unless ranges.empty?
        positive_queries << ranges.join(" OR ")
      end
      unless shard_ids.empty?
        positive_queries << "id IN (?)"
        conditions << shard_ids
      end
      positive_query = positive_queries.join(" OR ")
      scope = scope.where(positive_query, *conditions) unless positive_queries.empty?

      scope = scope.where("NOT (#{negative_ranges.join(" OR")})") unless negative_ranges.empty?
      scope = scope.where("id NOT IN (?)", negative_shard_ids) unless negative_shard_ids.empty?
      scope
    end

    def self.filter_database_servers_chain
      @filter_database_servers_chain ||= ->(servers) { servers }
    end
  end
end
