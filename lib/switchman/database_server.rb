# frozen_string_literal: true

require 'securerandom'

module Switchman
  class DatabaseServer
    attr_accessor :id

    class << self
      attr_accessor :creating_new_shard

      def all
        database_servers.values
      end

      def all_roles
        @all_roles ||= all.map(&:roles).flatten.uniq
      end

      def find(id_or_all)
        return all if id_or_all == :all
        return id_or_all.map { |id| database_servers[id || ::Rails.env] }.compact.uniq if id_or_all.is_a?(Array)

        database_servers[id_or_all || ::Rails.env]
      end

      def create(settings = {})
        raise 'database servers should be set up in database.yml' unless ::Rails.env.test?

        id = settings[:id]
        unless id
          @id ||= 0
          @id += 1
          id = @id
        end
        server = DatabaseServer.new(id.to_s, settings)
        server.instance_variable_set(:@fake, true)
        database_servers[server.id] = server
        ::ActiveRecord::Base.configurations.configurations <<
          ::ActiveRecord::DatabaseConfigurations::HashConfig.new(::Rails.env, "#{server.id}/primary", settings)
        Shard.send(:initialize_sharding)
        server
      end

      def server_for_new_shard
        servers = all.select { |s| s.config[:open] }
        return find(nil) if servers.empty?

        servers[rand(servers.length)]
      end

      private

      def reference_role(role)
        return if all_roles.include?(role)

        @all_roles << role
        Shard.send(:initialize_sharding)
      end

      def database_servers
        if !@database_servers || @database_servers.empty?
          @database_servers = {}.with_indifferent_access
          ::ActiveRecord::Base.configurations.configurations.each do |config|
            if config.name.include?('/')
              name, role = config.name.split('/')
            else
              name, role = config.env_name, config.name
            end

            if role == 'primary'
              @database_servers[name] = DatabaseServer.new(config.env_name, config.configuration_hash)
            else
              @database_servers[name].roles << role.to_sym
            end
          end
        end
        @database_servers
      end
    end

    attr_reader :roles

    def initialize(id = nil, config = {})
      @id = id
      @config = config.deep_symbolize_keys
      @configs = {}
      @roles = [:primary]
    end

    def connects_to_hash
      self.class.all_roles.to_h do |role|
        config_role = role
        config_role = :primary unless roles.include?(role)
        config_name = :"#{id}/#{config_role}"
        config_name = :primary if id == ::Rails.env && config_role == :primary
        [role.to_sym, config_name]
      end
    end

    def destroy
      self.class.send(:database_servers).delete(id) if id
      Shard.sharded_models.each do |klass|
        self.class.all_roles.each do |role|
          klass.connection_handler.remove_connection_pool(klass.connection_specification_name, role: role,
                                                                                               shard: id.to_sym)
        end
      end
    end

    def fake?
      @fake
    end

    def config(environment = :primary)
      @configs[environment] ||=
        case @config[environment]
        when Array
          @config[environment].map do |config|
            config = @config.merge((config || {}).symbolize_keys)
            # make sure GuardRail doesn't get any brilliant ideas about choosing the first possible server
            config.delete(environment)
            config
          end
        when Hash
          @config.merge(@config[environment])
        else
          @config
        end
    end

    # locks this db to a specific environment, except for
    # when doing writes (then it falls back to the current
    # value of GuardRail.environment)
    def guard!(environment = :secondary)
      ::ActiveRecord::Base.connected_to_stack << { shard_roles: { id.to_sym => environment }, klasses: [::ActiveRecord::Base] }
    end

    def unguard!
      ::ActiveRecord::Base.connected_to_stack << { shard_roles: { id.to_sym => :_switchman_inherit }, klasses: [::ActiveRecord::Base] }
    end

    def unguard
      return yield unless ::ActiveRecord::Base.current_role_overriden?

      begin
        unguard!
        yield
      ensure
        ::ActiveRecord::Base.connected_to_stack.pop
      end
    end

    def shards
      if id == ::Rails.env
        Shard.where('database_server_id IS NULL OR database_server_id=?', id)
      else
        Shard.where(database_server_id: id)
      end
    end

    def create_new_shard(id: nil, name: nil, schema: true)
      unless Shard.default.is_a?(Shard)
        raise NotImplementedError,
              "Cannot create new shards when sharding isn't initialized"
      end

      create_statement = -> { "CREATE SCHEMA #{name}" }
      password = " PASSWORD #{::ActiveRecord::Base.connection.quote(config[:password])}" if config[:password]
      sharding_config = Switchman.config
      config_create_statement = sharding_config[config[:adapter]]&.[](:create_statement)
      config_create_statement ||= sharding_config[:create_statement]
      if config_create_statement
        create_commands = Array(config_create_statement).dup
        create_statement = lambda {
          create_commands.map { |statement| format(statement, name: name, password: password) }
        }
      end

      id ||= begin
        id_seq = Shard.connection.quote(Shard.connection.quote_table_name('switchman_shards_id_seq'))
        next_id = Shard.connection.select_value("SELECT nextval(#{id_seq})")
        next_id.to_i
      end

      name ||= "#{config[:database]}_shard_#{id}"

      schema_already_existed = false
      shard = nil
      Shard.connection.transaction do
        self.class.creating_new_shard = true
        DatabaseServer.send(:reference_role, :deploy)
        ::ActiveRecord::Base.connected_to(shard: self.id.to_sym, role: :deploy) do
          shard = Shard.create!(id: id,
                                name: name,
                                database_server_id: self.id)
          if create_statement
            if ::ActiveRecord::Base.connection.select_value("SELECT 1 FROM pg_namespace WHERE nspname=#{::ActiveRecord::Base.connection.quote(name)}")
              schema_already_existed = true
              raise 'This schema already exists; cannot overwrite'
            end
            Array(create_statement.call).each do |stmt|
              ::ActiveRecord::Base.connection.execute(stmt)
            end
          end
          if config[:adapter] == 'postgresql'
            old_proc = ::ActiveRecord::Base.connection.raw_connection.set_notice_processor do
            end
          end
          old_verbose = ::ActiveRecord::Migration.verbose
          ::ActiveRecord::Migration.verbose = false

          unless schema == false
            shard.activate do
              reset_column_information

              ::ActiveRecord::Base.connection.transaction(requires_new: true) do
                ::ActiveRecord::Base.connection.migration_context.migrate
              end
              reset_column_information
              ::ActiveRecord::Base.descendants.reject do |m|
                m <= UnshardedRecord || !m.table_exists?
              end.each(&:define_attribute_methods)
            end
          end
        ensure
          ::ActiveRecord::Migration.verbose = old_verbose
          ::ActiveRecord::Base.connection.raw_connection.set_notice_processor(&old_proc) if old_proc
        end
        shard
      rescue
        shard&.destroy
        shard&.drop_database rescue nil unless schema_already_existed
        reset_column_information unless schema == false rescue nil
        raise
      ensure
        self.class.creating_new_shard = false
      end
    end

    def cache_store
      @cache_store ||= Switchman.config[:cache_map][id] || Switchman.config[:cache_map][::Rails.env]
      @cache_store
    end

    def shard_name(shard)
      return config[:shard_name] if config[:shard_name]

      if shard == :bootstrap
        # rescue nil because the database may not exist yet; if it doesn't,
        # it will shortly, and this will be re-invoked
        ::ActiveRecord::Base.connection.current_schemas.first rescue nil
      else
        shard.activate { ::ActiveRecord::Base.connection_pool.default_schema }
      end
    end

    def primary_shard
      unless instance_variable_defined?(:@primary_shard)
        # if sharding isn't fully set up yet, we may not be able to query the shards table
        @primary_shard = Shard.default if Shard.default.database_server == self
        @primary_shard ||= shards.where(name: nil).first
      end
      @primary_shard
    end

    private

    def reset_column_information
      ::ActiveRecord::Base.descendants.reject { |m| m <= UnshardedRecord }.each(&:reset_column_information)
    end
  end
end
