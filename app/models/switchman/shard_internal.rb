require 'switchman/database_server'
require 'switchman/default_shard'
require 'switchman/environment'
require 'switchman/errors'

module Switchman
  class Shard < ::ActiveRecord::Base
    # ten trillion possible ids per shard. yup.
    IDS_PER_SHARD = 10_000_000_000_000

    CATEGORIES =
      {
          # special cased to mean all other models
          :primary => nil,
          # special cased to not allow activating a shard other than the default
          :unsharded => [Shard]
      }
    private_constant :CATEGORIES
    @connection_specification_name = @shard_category = :unsharded

    if defined?(::ProtectedAttributes)
      attr_accessible :default, :name, :database_server
    end

    # only allow one default
    validates_uniqueness_of :default, :if => lambda { |s| s.default? }

    after_save :clear_cache
    after_destroy :clear_cache

    after_rollback :on_rollback

    scope :primary, -> { where(name: nil).order(:database_server_id, :id).distinct_on(:database_server_id) }

    class << self
      def categories
        CATEGORIES.keys
      end

      def default(reload_deprecated = false, reload: false, with_fallback: false)
        reload = reload_deprecated if reload_deprecated
        if !@default || reload
          # Have to create a dummy object so that several key methods still work
          # (it's easier to do this in one place here, and just assume that sharding
          # is up and running everywhere else).  This includes for looking up the
          # default shard itself. This also needs to be a local so that this method
          # can be re-entrant
          default = DefaultShard.instance

          # if we already have a default shard in place, and the caller wants
          # to use it as a fallback, use that instead of the dummy instance
          if with_fallback && @default
            default = @default
          end

          # the first time we need a dummy dummy for re-entrancy to avoid looping on ourselves
          @default ||= default

          # Now find the actual record, if it exists; rescue the fake default if the table doesn't exist
          @default = begin
            find_cached("default_shard") { Shard.where(default: true).take } || default
          rescue
            default
          end

          # rebuild current shard activations - it might have "another" default shard serialized there
          active_shards.replace(active_shards.map do |category, shard|
            shard = Shard.lookup((!shard || shard.default?) ? 'default' : shard.id)
            [category, shard]
          end.to_h)

          activate!(primary: @default) if active_shards.empty?

          # make sure this is not erroneously cached
          if @default.database_server.instance_variable_defined?(:@primary_shard)
            @default.database_server.remove_instance_variable(:@primary_shard)
          end
        end
        @default
      end

      def current(category = :primary)
        active_shards[category] || Shard.default
      end

      def activate(shards)
        old_shards = activate!(shards)
        yield
      ensure
        active_shards.merge!(old_shards) if old_shards
      end

      def activate!(shards)
        old_shards = nil
        currently_active_shards = active_shards
        shards.each do |category, shard|
          next if category == :unsharded
          unless currently_active_shards[category] == shard
            old_shards ||= {}
            old_shards[category] = currently_active_shards[category]
            currently_active_shards[category] = shard
          end
        end
        old_shards
      end

      def lookup(id)
        id_i = id.to_i
        return current if id_i == current.id || id == 'self'
        return default if id_i == default.id || id.nil? || id == 'default'
        id = id_i
        raise ArgumentError if id == 0

        unless cached_shards.has_key?(id)
          cached_shards[id] = Shard.default.activate do
            find_cached(['shard', id]) { find_by(id: id) }
          end
        end
        cached_shards[id]
      end

      def clear_cache
        cached_shards.clear
      end

      # ==== Parameters
      #
      # * +shards+ - an array or relation of Shards to iterate over
      # * +categories+ - an array of categories to activate
      # * +options+ -
      #    :parallel - true/false to execute in parallel, or a integer of how many
      #                sub-processes per database server. Note that parallel
      #                invocation currently uses forking, so should be used sparingly
      #                because errors are not raised, and you cannot get results back
      #    :max_procs - only run this many parallel processes at a time
      #    :exception - :ignore, :raise, :defer (wait until the end and raise the first
      #                error), or a proc
      def with_each_shard(*args)
        raise ArgumentError, "wrong number of arguments (#{args.length} for 0...3)" if args.length > 3

        unless default.is_a?(Shard)
          return Array.wrap(yield)
        end

        options = args.extract_options!
        if args.length == 1
          if Array === args.first && args.first.first.is_a?(Symbol)
            categories = args.first
          else
            scope = args.first
          end
        else
          scope, categories = args
        end

        parallel = case options[:parallel]
                     when true
                       1
                     when false, nil
                       0
                     else
                       options[:parallel]
                   end
        options.delete(:parallel)

        scope ||= Shard.all
        if ::ActiveRecord::Relation === scope && scope.order_values.empty?
          scope = scope.order(::Arel.sql("database_server_id IS NOT NULL, database_server_id, id"))
        end

        if parallel > 0
          max_procs = determine_max_procs(options.delete(:max_procs), parallel)
          if ::ActiveRecord::Relation === scope
            # still need a post-uniq, cause the default database server could be NULL or Rails.env in the db
            database_servers = scope.reorder('database_server_id').select(:database_server_id).distinct.
                map(&:database_server).compact.uniq
            parallel = [(max_procs.to_f / database_servers.count).ceil, parallel].min if max_procs

            scopes = Hash[database_servers.map do |server|
              server_scope = server.shards.merge(scope)
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
              parallel = [(max_procs.to_f / scopes.count).ceil, parallel].min if max_procs
              scopes = Hash[scopes.map do |(server, shards)|
                [server, shards.in_groups(parallel, false).compact]
              end]
            else
              scopes = Hash[scopes.map { |(server, shards)| [server, [shards]] }]
            end
          end

          exception_pipes = []
          pids = []
          out_fds = []
          err_fds = []
          pid_to_name_map = {}
          fd_to_name_map = {}
          errors = []

          wait_for_output = lambda do |out_fds, err_fds, fd_to_name_map|
            ready, _ = IO.select(out_fds + err_fds)
            ready.each do |fd|
              if fd.eof?
                fd.close
                out_fds.delete(fd)
                err_fds.delete(fd)
                next
              end
              line = fd.readline
              puts "#{fd_to_name_map[fd]}: #{line}"
            end
          end

          # only one process; don't bother forking
          if scopes.length == 1 && parallel == 1
            return with_each_shard(scopes.first.last.first, categories, options) { yield }
          end

          # clear connections prior to forking (no more queries will be executed in the parent,
          # and we want them gone so that we don't accidentally use them post-fork doing something
          # silly like dealloc'ing prepared statements)
          ::ActiveRecord::Base.clear_all_connections!

          scopes.each do |server, subscopes|
            subscopes.each_with_index do |subscope, idx|
              if subscopes.length > 1
                name = "#{server.id} #{idx + 1}"
              else
                name = server.id
              end

              exception_pipe = IO.pipe
              exception_pipes << exception_pipe
              pid, io_in, io_out, io_err = Open4.pfork4(lambda do
                begin
                  Switchman.config[:on_fork_proc]&.call
                  $0 = [$0, ARGV, name].flatten.join(' ')
                  with_each_shard(subscope, categories, options) { yield }
                  exception_pipe.last.close
                rescue => e
                  begin
                    dumped = Marshal.dump(e)
                  rescue
                    # couldn't dump the exception; create a copy with just
                    # the message and the backtrace
                    e2 = e.class.new(e.message)
                    e2.set_backtrace(e.backtrace)
                    e2.instance_variable_set(:@active_shards, e.instance_variable_get(:@active_shards))
                    dumped = Marshal.dump(e2)
                  end
                  exception_pipe.last.set_encoding(dumped.encoding)
                  exception_pipe.last.write(dumped)
                  exception_pipe.last.flush
                  exception_pipe.last.close
                  exit! 1
                end
              end)
              exception_pipe.last.close
              pids << pid
              io_in.close # don't care about writing to stdin
              out_fds << io_out
              err_fds << io_err
              pid_to_name_map[pid] = name
              fd_to_name_map[io_out] = name
              fd_to_name_map[io_err] = name

              while max_procs && pids.count >= max_procs
                while max_procs && out_fds.count >= max_procs
                  # wait for output if we've hit the max_procs limit
                  wait_for_output.call(out_fds, err_fds, fd_to_name_map)
                end
                # we've gotten all the output from one fd so wait for its child process to exit
                found_pid, status = Process.wait2
                pids.delete(found_pid)
                errors << pid_to_name_map[found_pid] if status.exitstatus != 0
              end
            end
          end

          while out_fds.any? || err_fds.any?
            wait_for_output.call(out_fds, err_fds, fd_to_name_map)
          end
          pids.each do |pid|
            _, status = Process.waitpid2(pid)
            errors << pid_to_name_map[pid] if status.exitstatus != 0
          end

          # check for an exception; we only re-raise the first one
          exception_pipes.each do |exception_pipe|
            begin
              serialized_exception = exception_pipe.first.read
              next if serialized_exception.empty?
              exception = Marshal.load(serialized_exception)
              raise exception
            ensure
              exception_pipe.first.close
            end
          end

          unless errors.empty?
            raise ParallelShardExecError.new("The following subprocesses did not exit cleanly: #{errors.sort.join(", ")}")
          end
          return
        end

        categories ||= []

        previous_shard = nil
        close_connections_if_needed = lambda do |shard|
          # prune the prior connection unless it happened to be the same
          if previous_shard && shard != previous_shard && !previous_shard.database_server.shareable?
            previous_shard.activate do
              ::Shackles.activated_environments.each do |env|
                ::Shackles.activate(env) do
                  if ::ActiveRecord::Base.connected? && ::ActiveRecord::Base.connection.open_transactions == 0
                    ::ActiveRecord::Base.connection_pool.current_pool.disconnect!
                  end
                end
              end
            end
          end
        end

        result = []
        exception = nil
        scope.each do |shard|
          # shard references a database server that isn't configured in this environment
          next unless shard.database_server
          close_connections_if_needed.call(shard)
          shard.activate(*categories) do
            begin
              result.concat Array.wrap(yield)
            rescue
              case options[:exception]
              when :ignore
              when :defer
                exception ||= $!
              when Proc
                options[:exception].call
              when :raise
                raise
              else
                raise
              end
            end
          end
          previous_shard = shard
        end
        close_connections_if_needed.call(Shard.current)
        raise exception if exception
        result
      end

      def partition_by_shard(array, partition_proc = nil)
        shard_arrays = {}
        array.each do |object|
          partition_object = partition_proc ? partition_proc.call(object) : object
          case partition_object
            when Shard
              shard = partition_object
            when ::ActiveRecord::Base
              if partition_object.respond_to?(:associated_shards)
                partition_object.associated_shards.each do |a_shard|
                  shard_arrays[a_shard] ||= []
                  shard_arrays[a_shard] << object
                end
                next
              else
                shard = partition_object.shard
              end
            when Integer, /^\d+$/, /^(\d+)~(\d+)$/
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
          results.concat shard.activate { Array.wrap(yield objects) }
        end
      end

      # converts an AR object, integral id, string id, or string short-global-id to a
      # integral id. nil if it can't be interpreted
      def integral_id_for(any_id)
        if any_id.is_a?(::Arel::Nodes::Casted)
          any_id = any_id.val
        elsif any_id.is_a?(::Arel::Nodes::BindParam) && ::Rails.version >= "5.2"
          any_id = any_id.value.value_before_type_cast
        end

        case any_id
        when ::ActiveRecord::Base
          any_id.id
        when /^(\d+)~(\d+)$/
          local_id = $2.to_i
          # doesn't make sense to have a double-global id
          return nil if local_id > IDS_PER_SHARD
          $1.to_i * IDS_PER_SHARD + local_id
        when Integer, /^\d+$/
          any_id.to_i
        else
          nil
        end
      end

      # takes an id-ish, and returns a local id and the shard it's
      # local to. [nil, nil] if it can't be interpreted. [id, nil]
      # if it's already a local ID. [nil, nil] if it's a well formed
      # id, but the shard it refers to does not exist
      NIL_NIL_ID = [nil, nil].freeze
      def local_id_for(any_id)
        id = integral_id_for(any_id)
        return NIL_NIL_ID unless id
        if id < IDS_PER_SHARD
          [id, nil]
        elsif shard = lookup(id / IDS_PER_SHARD)
          [id % IDS_PER_SHARD, shard]
        else
          NIL_NIL_ID
        end
      end

      # takes an id-ish, and returns an integral id relative to
      # target_shard. returns nil if it can't be interpreted,
      # or the integral version of the id if it refers to a shard
      # that does not exist
      def relative_id_for(any_id, source_shard, target_shard)
        integral_id = integral_id_for(any_id)
        local_id, shard = local_id_for(integral_id)
        return integral_id unless local_id
        shard ||= source_shard
        return local_id if shard == target_shard
        shard.global_id_for(local_id)
      end

      # takes an id-ish, and returns a shortened global
      # string id if global, and itself if local.
      # returns any_id itself if it can't be interpreted
      def short_id_for(any_id)
        local_id, shard = local_id_for(any_id)
        return any_id unless local_id
        return local_id unless shard
        "#{shard.id}~#{local_id}"
      end

      # takes an id-ish, and returns an integral global id.
      # returns nil if it can't be interpreted
      def global_id_for(any_id, source_shard = nil)
        id = integral_id_for(any_id)
        return any_id unless id
        if id >= IDS_PER_SHARD
          id
        else
          source_shard ||= Shard.current
          source_shard.global_id_for(id)
        end
      end

      def shard_for(any_id, source_shard = nil)
        return any_id.shard if any_id.is_a?(::ActiveRecord::Base)
        _, shard = local_id_for(any_id)
        shard || source_shard || Shard.current
      end

      # given the provided option, determines whether we need to (and whether
      # it's possible) to determine a reasonable default.
      def determine_max_procs(max_procs_input, parallel_input=2)
        max_procs = nil
        if max_procs_input
          max_procs = max_procs_input.to_i
          max_procs = nil if max_procs == 0
        else
          return 1 if parallel_input.nil? || parallel_input < 1
          cpus = Environment.cpu_count
          if cpus && cpus > 0
            max_procs = cpus * parallel_input
          end
        end

        return max_procs
      end

      private
      # in-process caching
      def cached_shards
        @cached_shards ||= {}.compare_by_identity
      end

      def add_to_cache(shard)
        cached_shards[shard.id] = shard
      end

      def remove_from_cache(shard)
        cached_shards.delete(shard.id)
      end

      def find_cached(key)
        # can't simply cache the AR object since Shard has a custom serializer
        # that calls this method
        attributes = Switchman.cache.fetch(key) { yield&.attributes }
        return nil unless attributes

        shard = Shard.new
        attributes.each do |attr, value|
          shard.send(:"#{attr}=", value) if shard.respond_to?(:"#{attr}=")
        end
        shard.clear_changes_information
        shard.instance_variable_set(:@new_record, false)
        # connection info doesn't exist in database.yml;
        # pretend the shard doesn't exist either
        shard = nil unless shard.database_server
        shard
      end

      def active_shards
        Thread.current[:active_shards] ||= {}.compare_by_identity
      end
    end

    def name
      unless instance_variable_defined?(:@name)
        # protect against re-entrancy
        @name = nil
        @name = read_attribute(:name) || default_name
      end
      @name
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

    def primary?
      self == database_server.primary_shard
    end

    def description
      [database_server.id, name].compact.join(':')
    end

    # Shards are always on the default shard
    def shard
      Shard.default
    end

    def activate(*categories)
      shards = hashify_categories(categories)
      Shard.activate(shards) do
        yield
      end
    end

    # for use from console ONLY
    def activate!(*categories)
      shards = hashify_categories(categories)
      Shard.activate!(shards)
      nil
    end

    # custom serialization, since shard is self-referential
    def _dump(depth)
      self.id.to_s
    end

    def self._load(str)
      lookup(str.to_i)
    end

    def drop_database
      raise("Cannot drop the database of the default shard") if self.default?
      return unless read_attribute(:name)

      begin
        adapter = self.database_server.config[:adapter]
        sharding_config = Switchman.config || {}
        drop_statement = sharding_config[adapter]&.[](:drop_statement)
        drop_statement ||= sharding_config[:drop_statement]
        if drop_statement
          drop_statement = Array(drop_statement).dup.
              map { |statement| statement.gsub('%{name}', self.name) }
        end

        case adapter
          when 'mysql', 'mysql2'
            self.activate do
              ::Shackles.activate(:deploy) do
                drop_statement ||= "DROP DATABASE #{self.name}"
                Array(drop_statement).each do |stmt|
                  ::ActiveRecord::Base.connection.execute(stmt)
                end
              end
            end
          when 'postgresql'
            self.activate do
              ::Shackles.activate(:deploy) do
                # Shut up, Postgres!
                conn = ::ActiveRecord::Base.connection
                old_proc = conn.raw_connection.set_notice_processor {}
                begin
                  drop_statement ||= "DROP SCHEMA #{self.name} CASCADE"
                  Array(drop_statement).each do |stmt|
                    ::ActiveRecord::Base.connection.execute(stmt)
                  end
                ensure
                  conn.raw_connection.set_notice_processor(&old_proc) if old_proc
                end
              end
            end
        end
      rescue
        logger.info "Drop failed: #{$!}"
      end
    end

    # takes an id local to this shard, and returns a global id
    def global_id_for(local_id)
      return nil unless local_id
      local_id + self.id * IDS_PER_SHARD
    end

    # skip global_id.hash
    def hash
      id.hash
    end

    def destroy
      raise("Cannot destroy the default shard") if self.default?
      super
    end

    private

    def clear_cache
      Shard.default.activate do
        Switchman.cache.delete(['shard', id].join('/'))
        Switchman.cache.delete("default_shard") if default?
      end
    end

    def default_name
      database_server.shard_name(self)
    end

    def on_rollback
      # make sure all connection pool proxies are referencing valid pools
      ::ActiveRecord::Base.connection_handler.connection_pools.each do |pool|
        next unless pool.is_a?(ConnectionPoolProxy)
        ::Shackles.activated_environments.each do |env|
          ::Shackles.activate(env) do
            pool.current_pool
          end
        end
      end
    end

    def hashify_categories(categories)
      if categories.empty?
        { :primary => self }
      else
        categories.inject({}) { |h, category| h[category] = self; h }
      end
    end

  end
end
