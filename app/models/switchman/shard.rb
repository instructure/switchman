require_dependency 'switchman/database_server'
require_dependency 'switchman/default_shard'

module Switchman
  class Shard < ActiveRecord::Base
    # ten trillion possible ids per shard. yup.
    IDS_PER_SHARD = 10_000_000_000_000

    CATEGORIES =
      {
          # special cased to mean all other models
          :default => nil,
          # special cased to not allow activating a shard other than the default
          :unsharded => Shard
      }

    attr_accessible :name, :database_server, :default

    # only allow one default
    validates_uniqueness_of :default, :if => lambda { |s| s.default? }

    after_save :clear_cache


    class << self
      def default(reload = false)
        if !@default || reload
          # Have to create a dummy object so that several key methods still work
          # (it's easier to do this in one place here, and just assume that sharding
          # is up and running everywhere else).  This includes for looking up the
          # default shard itself. This also needs to be a local so that this method
          # can be re-entrant
          default = Switchman::DefaultShard.new

          # the first time we need a dummy dummy for re-entrancy to avoid looping on ourselves
          @default ||= default

          if default.database_server.config[:adapter] == 'postgresql'
            # rescue nil because the database may not exist yet; if it doesn't, it will shortly, and we'll
            # be reloading anyway
            default.instance_variable_set(:@name, default.database_server.config[:shard_name] || (ActiveRecord::Base.connection.schemas.first rescue nil))
          else
            default.instance_variable_set(:@name, default.database_server.config[:shard_name] || default.database_server.config[:database])
          end

          # Now find the actual record, if it exists; rescue the fake default if the table doesn't exist
          @default = Shard.find_by_default(true) || default rescue default
        end
        @default
      end

      def current(category = :default)
        Thread.current[:active_shards] ||= {}
        result = Thread.current[:active_shards][category]
        result ||= Shard.default
      end

      def activate(shards)
        Thread.current[:active_shards] ||= {}
        old_shards = {}
        shards.each do |category, shard|
          next if category == :unsharded
          old_shards[category] = Thread.current[:active_shards][category]
          Thread.current[:active_shards][category] = shard
        end
        yield
      ensure
        old_shards.each do |category, shard|
          Thread.current[:active_shards][category] = shard
        end
      end

      def lookup(id)
        id_i = id.to_i
        return current if id_i == current.id || id == 'self'
        return default if id_i == default.id || id.nil? || id == 'default'
        id = id_i
        raise ArgumentError if id == 0

        cached_shards[id] ||= Shard.default.activate do
          # can't simply cache the AR object since Shard has a custom serializer
          # that calls this method
          attributes = Rails.cache.fetch(['shard', id].join('/')) do
            shard = find_by_id(id)
            shard.try(:attributes) || :nil
          end
          if attributes == :nil
            nil
          else
            shard = Shard.new
            shard.send(:attributes=, attributes, false)
            shard.instance_variable_set(:@new_record, false)
            # connection info doesn't exist in database.yml;
            # pretend the shard doesn't exist either
            shard = nil unless shard.database_server
            shard
          end
        end
      end

      def clear_cache
        @cached_shards = {}
      end

      # options
      #  :parallel - true/false to execute in parallel, or a integer of how many
      #              sub-processes per database server. Note that parallel
      #              invocation currently uses forking, so should be used sparingly
      #              because errors are not raised, and you cannot get results back
      def with_each_shard(scope = nil, categories = nil, options = {})
        unless default.is_a?(Shard)
          return Array(yield)
        end

        parallel = case options[:parallel]
                     when true
                       1
                     when false, nil
                       0
                     else
                       options[:parallel]
                   end
        scope ||= Shard.order("database_server_id IS NOT NULL, database_server_id, id")

        if parallel > 0
          if scope.class == ActiveRecord::NamedScope::Scope
            # still need a post-uniq, cause the default database server could be NULL or Rails.env in the db
            database_servers = scope.reorder('database_server_id').select(:database_server_id).uniq.
                map(&:database_server).compact.uniq
            scopes = Hash[database_servers.map do |server|
              server_scope = server.shards(scope)
              if parallel == 1
                subscopes = [server_scope]
              else
                subscopes = []
                total = server_scope.count
                ranges = []
                server_scope.find_ids_in_ranges(:batch_size => (total.to_f / parallel).ceil) do |min, max|
                  ranges << [min, max]
                end
                # create a half-open range on the last one
                ranges.last[1] = nil
                ranges.each do |min, max|
                  subscope = server_scope.where("id>=?", min)
                  subscope = subscope.where("id<=?", max) if max
                  subscopes << subscope
                end
              end
              [server, subscopes]
            end]
          else
            scopes = scope.group_by(&:database_server)
            if parallel > 1
              scopes = Hash[scopes.map do |(server, shards)|
                [server, shards.in_groups(parallel, false).compact]
              end]
            end
          end

          fd_to_name_map = {}
          fds = []
          pids = []
          exception_pipe = IO.pipe
          scopes.each do |server, subscopes|
            if subscopes.first.class != ActiveRecord::NamedScope::Scope && subscopes.first.class != Array
              subscopes = [subscopes]
            end
            # only one process; don't bother forking
            if scopes.length == 1 && subscopes.length == 1
              exception_pipe.first.close
              exception_pipe.last.close
              return with_each_shard(subscopes.first, categories) { yield }
            end
            subscopes.each_with_index do |subscope, idx|
              details = Open4.pfork4(lambda do
                begin
                  ActiveRecord::Base.clear_all_connections!
                  with_each_shard(subscope, categories) { yield }
                rescue Exception => e
                  exception_pipe.last.write(Marshal.dump(e))
                  exception_pipe.last.flush
                  exit 1
                end
              end)
              # don't care about writing to stdin
              details[1].close
              fds.concat details[2..3]
              pids << details[0]
              if subscopes.length > 1
                name = "#{server.id} #{idx + 1}"
              else
                name = server.id
              end
              fd_to_name_map[details[2]] = name
              fd_to_name_map[details[3]] = name
            end
          end
          exception_pipe.last.close

          while !fds.empty?
            ready, _ = IO.select(fds)
            ready.each do |fd|
              if fd.eof?
                fd.close
                fds.delete(fd)
                next
              end
              line = fd.readline
              puts "#{fd_to_name_map[fd]}: #{line}"
            end
          end
          pids.each { |pid| Process.waitpid2(pid) }
          # I'm not sure why, but we have to do this
          ActiveRecord::Base.clear_all_connections!
          # check for an exception; we only re-raise the first one
          # (all the sub-processes shared the same pipe, so we only
          # have to check the one)
          begin
            exception = Marshal.load exception_pipe.first
            raise exception
          rescue EOFError
            # No exceptions
          ensure
            exception_pipe.first.close
          end
          return
        end

        previous_shard = nil
        close_connections = lambda do
          previous_shard.activate { ActiveRecord::Base.connection_pool.current_pool.disconnect! if ActiveRecord::Base.connected? && ActiveRecord::Base.connection.open_transactions == 0 }
        end

        categories ||= [:default]

        result = []
        scope.each do |shard|
          # shard references a database server that isn't configured in this environment
          next unless shard.database_server
          # prune the prior connection unless it happened to be the same
          if previous_shard &&
              (shard.database_server != previous_shard.database_server || !previous_shard.database_server.shareable?)
            close_connections.call
          end
          active_categories = Hash[*categories.map { |category| [category, shard] }.flatten]
          Shard.activate(active_categories) do
            result.concat Array(yield)
          end
          previous_shard = shard
        end
        if previous_shard && previous_shard != Shard.current
          if Shard.current.database_server != previous_shard.database_server || !previous_shard.database_server.shareable?
            close_connections.call
          end
        end
        result
      end

      def partition_by_shard(array, partition_proc = nil)
        shard_arrays = {}
        array.each do |object|
          partition_object = partition_proc ? partition_proc.call(object) : object
          case partition_object
            when Shard
              shard = partition_object
            when ActiveRecord::Base
              shard = partition_object.shard
            when Fixnum, /^\d+$/, /^(\d+)~(\d+)$/
              local_id, shard = Shard.local_id_for(partition_object)
              local_id ||= partition_object
              object = local_id if !partition_proc
          end
          shard ||= Shard.current
          shard_arrays[shard] ||= []
          shard_arrays[shard] << object
        end
        # TODO: use with_each_shard (or vice versa) to get
        # connection management and parallelism benefits
        shard_arrays.inject([]) do |results, (shard, objects)|
          results.concat shard.activate { Array(yield objects) }
        end
      end

      def local_id_for(any_id)
        case any_id
          when /^(\d+)~(\d+)$/
            if shard = Shard.lookup($1.to_i)
              [$2.to_i, shard]
            else
              [nil, nil]
            end
          when Fixnum, /^\d+$/
            any_id = any_id.to_i
            if any_id < IDS_PER_SHARD
              [any_id, nil]
            elsif shard = lookup(any_id / IDS_PER_SHARD)
              [any_id % IDS_PER_SHARD, shard]
            else
              [nil, nil]
            end
          else
            [nil, nil]
        end
      end

      def relative_id_for(any_id, target_shard = nil)
        local_id, shard = local_id_for(any_id)
        shard ||= Shard.current
        local_id ? shard.relative_id_for(local_id, target_shard) : any_id
      end

      def global_id_for(any_id)
        if any_id >= IDS_PER_SHARD
          any_id
        else
          Shard.current.global_id_for(any_id)
        end
      end

      def shard_for(any_id, source_shard = Shard.current)
        id = source_shard.relative_id_for(any_id, Shard.default)
        return Shard.default if id < IDS_PER_SHARD
        lookup(id / IDS_PER_SHARD)
      end

      private
      # in-process caching
      def cached_shards
        @cached_shards ||= []
      end

      def add_to_cache(shard)
        cached_shards[shard.id] = shard
      end

      def remove_from_cache(shard)
        cached_shards.delete(shard.id)
      end
    end

    def name
      name = read_attribute(:name)
      if !name
        return @name if instance_variable_defined?(:@name)
        config = database_server.config
        @name = self.database_server.config[:shard_name]
        if config[:adapter] == 'postgresql'
          @name ||= self.activate { ActiveRecord::Base.connection_pool.default_schema }
          return @name
        else
          return @name ||= config[:database]
        end
      end
      name
    end

    def name=(name)
      write_attribute(:name, @name = name)
      remove_instance_variable(:@name) if name == nil
    end

    def database_server
      @database_server ||= DatabaseServer.find(self.database_server_id)
    end

    def database_server=(database_server)
      self.database_server_id = database_server.id
      @database_server = database_server
    end

    def description
      [database_server.id, name].compact.join(':')
    end

    # Shards are always on the default shard
    def shard
      Shard.default
    end

    def activate(*categories)
      categories = categories.flatten
      if categories.length <= 1
        Shard.activate((categories.first || :default) => self) { yield }
      else
        Shard.activate(Hash[*categories.map { |category| [category, self] }.flatten]) { yield }
      end
    end

    # for use from console ONLY
    def activate!(*categories)
      if categories.empty?
        categories = [:default]
      end
      categories.flatten.each do |category|
        next if category == :unsharded
        Thread.current[:active_shards][category] = self
      end
      nil
    end

    # custom serialization, since shard is self-referential
    def _dump(depth)
      self.local_id.to_s
    end

    def self._load(str)
      lookup(str.to_i)
    end

    def relative_id_for(any_id, target_shard = nil)
      target_shard ||= Shard.current
      return nil unless any_id
      # local id
      if any_id < IDS_PER_SHARD
        # still local; return unchanged
        return any_id if target_shard == self
        # otherwise a global id
        any_id + self.id * IDS_PER_SHARD
      else
        shard_id = any_id / IDS_PER_SHARD
        # global id that should be local
        return any_id % IDS_PER_SHARD if shard_id == target_shard.id
        # non-matching shard, return as global id
        any_id
      end
    end

    def global_id_for(local_id)
      return nil unless local_id
      local_id + self.id * IDS_PER_SHARD
    end

    private

    def clear_cache
      Shard.default.activate do
        Rails.cache.delete(['shard', id].join('/'))
      end
    end

  end
end
