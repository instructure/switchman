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

          Shard.with_each_shard(scope, Shard.categories, :parallel => ENV['PARALLEL'].to_i, :max_procs => ENV['MAX_PARALLEL_PROCS']) do
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

    def self.none(scope)
      if ::Rails.version < '4'
        scope.where("?", false)
      else
        scope.none
      end
    end

    def self.shard_scope(scope, raw_shard_ids)
      raw_shard_ids = raw_shard_ids.split(',')

      shard_ids = []
      negative_shard_ids = []
      ranges = []
      negative_ranges = []
      total_shard_count = nil

      raw_shard_ids.each do |id|
        case id
        when 'default'
          shard_ids << Shard.default.id
        when '-default'
          negative_shard_ids << Shard.default.id
        when 'primary'
          shard_ids.concat(Shard.primary.pluck(:id))
        when '-primary'
          negative_shard_ids.concat(Shard.primary.pluck(:id))
        when /^(-?)(\d+)?\.\.(\.)?(\d+)?$/
          negative, start, open, finish = $1.present?, $2, $3.present?, $4
          raise "Invalid shard id or range: #{id}" unless start || finish
          range = []
          range << "id>=#{start}" if start
          range << "id<#{'=' unless open}#{finish}" if finish
          (negative ? negative_ranges : ranges) << "(#{range.join(' AND ')})"
        when /^-(\d+)$/
          negative_shard_ids << $1.to_i
        when /^\d+$/
          shard_ids << id.to_i
        when %r{^(-?\d+)/(\d+)$}
          numerator = $1.to_i
          denominator = $2.to_i
          if numerator == 0 || numerator.abs > denominator
            raise "Invalid fractional chunk: #{id}"
          end
          # one chunk means everything
          if denominator == 1
            next if numerator == 1
            return none(scope)
          end

          total_shard_count ||= scope.count
          per_chunk = (total_shard_count / denominator.to_f).ceil
          index = numerator.abs

          # more chunks than shards; the trailing chunks are all empty
          return none(scope) if index > total_shard_count

          subscope = Shard.select(:id).order(:id)
          select = []
          if index != 1
            subscope = subscope.offset(per_chunk * (index - 1))
            select << "MIN(id) AS min_id"
          end
          if index != denominator
            subscope = subscope.limit(per_chunk)
            select << "MAX(id) AS max_id"
          end

          outerscope = if ::Rails.version < '4'
                         Shard.from("(#{subscope.to_sql}) subquery")
                       else
                         Shard.from(subscope)
                       end
          result = outerscope.select(select.join(", ")).to_a.first
          if index == 1
            range = "id<=#{result['max_id']}"
          elsif index == denominator
            range = "id>=#{result['min_id']}"
          else
            range = "(id>=#{result['min_id']} AND id<=#{result['max_id']})"
          end

          (numerator < 0 ? negative_ranges : ranges) <<  range
          else
          raise "Invalid shard id or range: #{id}"
        end
      end

      shard_ids.uniq!
      negative_shard_ids.uniq!
      unless shard_ids.empty?
        shard_ids -= negative_shard_ids
        if shard_ids.empty? && ranges.empty?
          return none(scope)
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
