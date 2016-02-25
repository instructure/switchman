module Switchman
  module Rake
    def self.filter_database_servers(&block)
      chain = filter_database_servers_chain # use a local variable so that the current chain is closed over in the following lambda
      @filter_database_servers_chain = lambda { |servers| block.call(servers, chain) }
    end

    def self.scope(base_scope = Shard,
      database_server: ENV['DATABASE_SERVER'],
      shard: ENV['SHARD'])
      servers = DatabaseServer.all

      if database_server
        servers = database_server
        if servers.first == '-'
          negative = true
          servers = servers[1..-1]
        end
        servers = servers.split(',')
        open = servers.delete('open')

        servers = servers.map { |server| DatabaseServer.find(server) }.compact
        servers.concat(DatabaseServer.all.select { |server| server.config[:open] }) if open
        servers = DatabaseServer.all - servers if negative
      end

      servers = filter_database_servers_chain.call(servers)

      scope = base_scope.order("database_server_id IS NOT NULL, database_server_id, id")
      if servers != DatabaseServer.all
        conditions = ["database_server_id IN (?)", servers.map(&:id)]
        conditions.first << " OR database_server_id IS NULL" if servers.include?(Shard.default.database_server)
        scope = scope.where(conditions)
      end

      if shard
        scope = shard_scope(scope, shard)
      end

      scope
    end

    def self.options
      { parallel: ENV['PARALLEL'].to_i, max_procs: ENV['MAX_PARALLEL_PROCS'] }
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
          Shard.with_each_shard(scope, Shard.categories, options) do
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
            return scope.none
          end

          total_shard_count ||= scope.count
          per_chunk = (total_shard_count / denominator.to_f).ceil
          index = numerator.abs

          # more chunks than shards; the trailing chunks are all empty
          return scope.none if index > total_shard_count

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

          result = Shard.from(subscope).select(select.join(", ")).to_a.first
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
          return scope.none
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

module Switchman
  module ActiveRecord
    module PostgreSQLDatabaseTasks
      if ::Rails.version < '4.2'
        def structure_dump(filename)
          set_psql_env
          search_path = configuration['schema_search_path']
          unless search_path.blank?
            search_path = search_path.split(",").map{|search_path_part| "--schema=#{Shellwords.escape(search_path_part.strip)}" }.join(" ")
            serialized_search_path = ::ActiveRecord::Base.connection.schema_search_path
          end
          if configuration['use_qualified_names']
            shard = Shard.current.name
            serialized_search_path = shard
            search_path = "--schema=#{Shellwords.escape(shard)}"
          end

          command = "pg_dump -s -x -O -f #{Shellwords.escape(filename)} #{search_path} #{Shellwords.escape(configuration['database'])}"
          raise 'Error dumping database' unless Kernel.system(command)

          File.open(filename, "a") { |f| f << "SET search_path TO #{serialized_search_path};\n\n" }
        end
      else
        def structure_dump(filename)
          set_psql_env
          args = ['-s', '-x', '-O', '-f', filename]
          search_path = configuration['schema_search_path']
          if configuration['use_qualified_names']
            shard = Shard.current.name
            serialized_search_path = shard
            args << "--schema=#{Shellwords.escape(shard)}"
          elsif !search_path.blank?
            args << search_path.split(',').map do |part|
              "--schema=#{part.strip}"
            end.join(' ')
            serialized_search_path = connection.schema_search_path
          end

          args << configuration['database']
          run_cmd('pg_dump', args, 'dumping')
          File.open(filename, "a") { |f| f << "SET search_path TO #{serialized_search_path};\n\n" }
        end
      end
    end
  end
end

ActiveRecord::Tasks::PostgreSQLDatabaseTasks.prepend(Switchman::ActiveRecord::PostgreSQLDatabaseTasks)
