# frozen_string_literal: true

module Switchman
  module Rake
    def self.filter_database_servers
      # use a local variable so that the current chain is closed over in the following lambda
      chain = filter_database_servers_chain
      @filter_database_servers_chain = ->(servers) { yield(servers, chain) }
    end

    def self.scope(base_scope = Shard,
                   database_server: ENV.fetch("DATABASE_SERVER", nil),
                   shard: ENV.fetch("SHARD", nil))
      servers = DatabaseServer.all

      if database_server
        servers = database_server
        if servers.first == "-"
          negative = true
          servers = servers[1..]
        end
        servers = servers.split(",")
        open = servers.delete("open")

        servers = servers.filter_map { |server| DatabaseServer.find(server) }
        if open
          open_servers = DatabaseServer.select { |server| server.config[:open] }
          servers.concat(open_servers)
          servers << DatabaseServer.find(nil) if open_servers.empty?
          servers.uniq!
        end
        servers = DatabaseServer.all - servers if negative
      end

      ENV["REGION"]&.split(",")&.each do |region|
        method = :select!
        if region[0] == "-"
          method = :reject!
          region = region[1..]
        end
        if region == "self"
          servers.send(method, &:in_current_region?)
        else
          servers.send(method) { |server| server.in_region?(region) }
        end
      end

      servers = filter_database_servers_chain.call(servers)

      scope = base_scope.order(::Arel.sql("database_server_id IS NOT NULL, database_server_id, id"))
      if servers != DatabaseServer.all
        database_server_ids = servers.map(&:id)
        database_server_ids << nil if servers.include?(Shard.default.database_server)
        scope = scope.where(database_server_id: database_server_ids)
      end

      scope = shard_scope(scope, shard) if shard

      scope
    end

    def self.options
      { exception: (ENV["FAIL_FAST"] == "0") ? :defer : :raise, parallel: ENV["PARALLEL"].to_i }
    end

    # classes - an array or proc, to activate as the current shard during the
    # task.
    def self.shardify_task(task_name, classes: [::ActiveRecord::Base])
      log_format = ENV.fetch("LOG_FORMAT", nil)
      old_task = ::Rake::Task[task_name]
      old_actions = old_task.actions.dup
      old_task.actions.clear

      old_task.enhance do |*task_args|
        if ::Rails.env.test?
          require "switchman/test_helper"
          TestHelper.recreate_persistent_test_shards(dont_create: true)
        end

        ::GuardRail.activate(:deploy) do
          Shard.default.database_server.unguard do
            classes = classes.call if classes.respond_to?(:call)

            # We don't want the shard status messages to be wrapped using a custom log transfomer
            original_stderr = $stderr
            original_stdout = $stdout
            output = if log_format == "json"
                       lambda { |msg|
                         JSON.dump(shard: Shard.current.id,
                                   database_server: Shard.current.database_server.id,
                                   type: "log",
                                   message: msg)
                       }
                     else
                       nil
                     end
            Shard.with_each_shard(scope, classes, output:, **options) do
              shard = Shard.current

              if log_format == "json"
                original_stdout.puts JSON.dump(
                  shard: shard.id,
                  database_server: shard.database_server.id,
                  type: "started"
                )
              else
                original_stdout.puts "#{shard.id}: #{shard.description}"
              end

              shard.database_server.unguard do
                old_actions.each { |action| action.call(*task_args) }
              end

              if log_format == "json"
                original_stdout.puts JSON.dump(
                  shard: shard.id,
                  database_server: shard.database_server.id,
                  type: "completed"
                )
              end
              nil
            rescue => e
              if log_format == "json"
                original_stderr.puts JSON.dump(
                  shard: shard.id,
                  database_server: shard.database_server.id,
                  type: "failed",
                  message: e.full_message
                )
              end

              raise
            end
          rescue => e
            if options[:parallel] != 0
              warn "Exception from #{e.current_shard.id}: #{e.current_shard.description}:\n#{e.full_message}"
            end
            raise
          end
        end
      end
    end

    %w[db:migrate db:migrate:up db:migrate:down db:rollback].each do |task_name|
      shardify_task(task_name)
    end

    def self.shard_scope(scope, raw_shard_ids)
      raw_shard_ids = raw_shard_ids.split(",")

      shard_ids = []
      negative_shard_ids = []
      ranges = []
      negative_ranges = []
      total_shard_count = nil

      raw_shard_ids.each do |id|
        case id
        when "default"
          shard_ids << Shard.default.id
        when "-default"
          negative_shard_ids << Shard.default.id
        when "primary"
          shard_ids.concat(Shard.primary.pluck(:id))
        when "-primary"
          negative_shard_ids.concat(Shard.primary.pluck(:id))
        when /^(-?)(\d+)?\.\.(\.)?(\d+)?$/
          negative, start, open, finish = $1.present?, $2, $3.present?, $4
          raise "Invalid shard id or range: #{id}" unless start || finish

          range = []
          range << "id>=#{start}" if start
          range << "id<#{"=" unless open}#{finish}" if finish
          (negative ? negative_ranges : ranges) << "(#{range.join(" AND ")})"
        when /^-(\d+)$/
          negative_shard_ids << $1.to_i
        when /^\d+$/
          shard_ids << id.to_i
        when %r{^(-?\d+)/(\d+)$}
          numerator = $1.to_i
          denominator = $2.to_i
          raise "Invalid fractional chunk: #{id}" if numerator.zero? || numerator.abs > denominator

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
          range = case index
                  when 1
                    "id<=#{result["max_id"]}"
                  when denominator
                    "id>=#{result["min_id"]}"
                  else
                    "(id>=#{result["min_id"]} AND id<=#{result["max_id"]})"
                  end

          (numerator.negative? ? negative_ranges : ranges) << range
        else
          raise "Invalid shard id or range: #{id}"
        end
      end

      shard_ids.uniq!
      negative_shard_ids.uniq!
      unless shard_ids.empty?
        shard_ids -= negative_shard_ids
        return scope.none if shard_ids.empty? && ranges.empty?

        # we already trimmed them all out; no need to make the server do it as well
        negative_shard_ids = [] if ranges.empty?
      end

      conditions = []
      positive_queries = []
      positive_queries << ranges.join(" OR ") unless ranges.empty?
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

  module ActiveRecord
    module PostgreSQLDatabaseTasks
      def structure_dump(...)
        ::ActiveRecord.dump_schemas = Switchman::Shard.current.name
        super
      end
    end
  end
end

ActiveRecord::Tasks::PostgreSQLDatabaseTasks.prepend(Switchman::ActiveRecord::PostgreSQLDatabaseTasks)
