module Switchman
  def self.shardify_task(task_name)
    old_task = Rake::Task[task_name]
    old_actions = old_task.actions.dup
    old_task.actions.clear

    old_task.enhance do
      ::Shackles.activate(:deploy) do

        scope = Shard.order("database_server_id IS NOT NULL, database_server_id, id")
        if ENV['DATABASE_SERVER']
          servers = ENV['DATABASE_SERVER']
          if servers.first == '-'
            negative = true
            servers = servers[1..-1]
          end
          servers = servers.split(',')
          conditions = ["database_server_id #{ "NOT " if negative }IN (?)", servers]
          conditions.first << " OR database_server_id IS NULL" if servers.include?(Rails.env) && !negative || !servers.include?(Rails.env) && negative
          scope = scope.where(conditions)
        end

        if ENV['SHARD']
          raw_shard_ids = ENV['SHARD'].split(',')
          shards = []
          default = false
          shard_ids = []
          ranges = []
          raw_shard_ids.each do |id|
            if id == 'default'
              default = true
            elsif id =~ /(\d+)?\.\.(\.)?(\d+)?/
              raise "Invalid shard id or range: #{id}" unless $1 || $3
              range = []
              range << "id>=#{$1}" if $1
              range << "id<#{'=' unless $2}#{$3}" if $3
              ranges << "(#{range.join(' AND ')})"
            elsif id =~ /\d+/
              shard_ids << id.to_i
            else
              raise "Invalid shard id or range: #{id}"
            end
          end
          queries = 0
          default_on_servers = !servers || servers.include?(Shard.default.database_server.id)
          default_on_servers = !default_on_servers if negative
          if default && default_on_servers
            shards << Shard.default
            queries += 1
          end
          shards.concat(scope.where(:id => shard_ids).all) unless shard_ids.empty?
          queries += 1 unless shard_ids.empty?
          shards.concat(scope.where(ranges.join(" OR ")).all) unless ranges.empty?
          queries += 1 unless ranges.empty?
          shards = shards.uniq.sort_by { |shard| [shard.database_server.id, shard.id] } if queries > 1
        end
        shards ||= scope

        Shard.with_each_shard(shards, Shard.categories, :parallel => ENV['PARALLEL'].to_i) do
          shard = Shard.current
          puts "#{shard.id}: #{shard.description}"
          shard.database_server.unshackle do
            old_actions.each(&:call)
          end
          nil
        end
      end
    end
  end

  %w{db:migrate db:migrate:up db:migrate:down db:rollback}.each { |task_name| shardify_task(task_name) }
end
