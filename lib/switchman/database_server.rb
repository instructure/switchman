module Switchman
  class DatabaseServer
    attr_accessor :id

    class << self
      def all
        database_servers.values
      end

      def find(id_or_all)
        return self.all if id_or_all == :all
        return id_or_all.map { |id| self.database_servers[id || ::Rails.env] }.compact.uniq if id_or_all.is_a?(Array)
        database_servers[id_or_all || ::Rails.env]
      end

      def create(settings = {})
        raise "database servers should be set up in database.yml" unless ::Rails.env.test?
        id = 1
        while database_servers[id.to_s]; id += 1; end
        server = DatabaseServer.new({ :id => id.to_s }.merge(settings))
        server.instance_variable_set(:@fake, true)
        raise "database server #{server.id} already exists" if database_servers[server.id]
        database_servers[server.id] = server
      end

      def server_for_new_shard
        servers = all.select { |s| s.config[:open] }
        return find(nil) if servers.empty?
        servers[rand(servers.length)]
      end

      private
      def database_servers
        unless @database_servers
          @database_servers = {}.with_indifferent_access
          ::ActiveRecord::Base.configurations.each do |(id, config)|
            @database_servers[id] = DatabaseServer.new(:id => id, :config => config)
          end
        end
        @database_servers
      end
    end

    def initialize(settings = {})
      @id = settings[:id]
      @config = (settings[:config] || {}).symbolize_keys
      @configs = {}
    end

    def destroy
      raise "database servers should be set up in database.yml" unless ::Rails.env.test?
      self.class.send(:database_servers).delete(self.id) if self.id
    end

    def fake?
      @fake
    end

    def config(environment = :master)
      @configs[environment] ||= begin
        if @config[environment].is_a?(Array)
          @config[environment].map do |config|
            config = @config.merge((config || {}).symbolize_keys)
            # make sure Shackles doesn't get any brilliant ideas about choosing the first possible server
            config.delete(environment)
            config
          end
        elsif @config[environment].is_a?(Hash)
          @config.merge(@config[environment])
        else
          @config
        end
      end
    end

    def shackles_environment
      @shackles_environment || ::Shackles.environment
    end

    # locks this db to a specific environment, except for
    # when doing writes (then it falls back to the current
    # value of Shackles.environment)
    def shackle!(environment = :slave)
      @shackles_environment = environment
    end

    def unshackle!
      @shackles_environment = nil
    end

    def unshackle
      old_env = @shackles_environment
      unshackle!
      yield
    ensure
      shackle!(old_env)
    end

    def shareable?
      @shareable_environment_key ||= []
      environment = ::Shackles.environment
      explicit_user = ::Shackles.global_config[:username]
      return @shareable if @shareable_environment_key == [environment, explicit_user]
      @shareable_environment_key = [environment, explicit_user]
      if explicit_user
        username = explicit_user
      else
        config = self.config(environment)
        config = config.first if config.is_a?(Array)
        username = config[:username]
      end
      @shareable = self.config[:adapter] != 'sqlite3' && username !~ /%?\{[a-zA-Z0-9_]+\}/
    end

    def shards
      if self.id == ::Rails.env
        Shard.where("database_server_id IS NULL OR database_server_id=?", self.id)
      else
        Shard.where(:database_server_id => self.id)
      end
    end

    def pool_key
      self.id == ::Rails.env ? nil : self.id
    end

    def create_new_shard(options = {})
      raise MethodNotImplemented unless Shard.default.is_a?(Shard)

      db_name = options[:db_name]
      create_schema = options[:schema]
      # look for another shard associated with this db
      other_shard = self.shards.where("name<>':memory:' OR name IS NULL").order(:id).first
      temp_db_name = other_shard.try(:name) unless id == ::Rails.env
      temp_db_name = Shard.default.name if id == ::Rails.env

      case config[:adapter]
        when 'postgresql'
          temp_db_name ||= 'public'
          create_statement = lambda { "CREATE SCHEMA #{db_name}" }
          password = " PASSWORD #{::ActiveRecord::Base.connection.quote(config[:password])}" if config[:password]
        when 'sqlite3'
          if db_name
            # Try to create a db on-disk even if the only shards for sqlite are in-memory
            temp_db_name = nil if temp_db_name == ':memory:'
            # Put it in the db directory if there are no other sqlite shards
            temp_db_name ||= 'db/dummy'
            temp_db_name = File.join(File.dirname(temp_db_name), "#{db_name}.sqlite3")
            # If they really asked for :memory:, give them :memory:
            temp_db_name = db_name if db_name == ':memory:'
            db_name = temp_db_name
          end
        else
          temp_db_name ||= self.config[:database] % self.config
          create_statement = lambda { "CREATE DATABASE #{db_name}" }
      end
      sharding_config = Switchman.config
      config_create_statement = sharding_config[config[:adapter]].try(:[], :create_statement)
      config_create_statement ||= sharding_config[:create_statement]
      if config_create_statement
        create_commands = Array(config_create_statement).dup
        create_statement = lambda {
          create_commands.map { |statement| statement.gsub('%{db_name}', db_name).gsub('%{password}', password || '') }
        }
      end

      create_shard = lambda do
        shard = Shard.create!(:name => temp_db_name,
                            :database_server => self) do |shard|
          shard.id = options[:id] if options[:id]
        end
        begin
          if db_name.nil?
            base_db_name = self.config[:database] % self.config
            base_db_name = $1 if base_db_name =~ /(?:.*\/)(.+)_shard_\d+(?:\.sqlite3)?$/
            base_db_name = nil if base_db_name == ':memory:'
            base_db_name << '_' if base_db_name
            db_name = "#{base_db_name}shard_#{shard.id}"
            if config[:adapter] == 'sqlite3'
              # Try to create a db on-disk even if the only shards for sqlite are in-memory
              temp_db_name = nil if temp_db_name == ':memory:'
              # Put it in the db directory if there are no other sqlite shards
              temp_db_name ||= 'db/dummy'
              db_name = File.join(File.dirname(temp_db_name), "#{db_name}.sqlite3")
              shard.name = db_name
            end
          end
          shard.activate(Shard.categories) do
            ::Shackles.activate(:deploy) do
              begin
                if create_statement
                  Array(create_statement.call).each do |stmt|
                    ::ActiveRecord::Base.connection.execute(stmt)
                  end
                  # have to disconnect and reconnect to the correct db
                  if self.shareable? && other_shard
                    other_shard.activate { ::ActiveRecord::Base.connection }
                  else
                    ::ActiveRecord::Base.connection_pool.current_pool.disconnect!
                  end
                end
                shard.name = db_name
                old_proc = ::ActiveRecord::Base.connection.raw_connection.set_notice_processor {} if config[:adapter] == 'postgresql'
                old_verbose = ::ActiveRecord::Migration.verbose
                ::ActiveRecord::Migration.verbose = false

                reset_column_information
                ::ActiveRecord::Migrator.migrate(::Rails.root + "db/migrate/") unless create_schema == false
                reset_column_information
              ensure
                ::ActiveRecord::Migration.verbose = old_verbose
                ::ActiveRecord::Base.connection.raw_connection.set_notice_processor(&old_proc) if old_proc
              end
            end
          end
          shard.save!
          shard
        rescue
          shard.destroy
          shard.drop_database if shard.name == db_name rescue nil
          raise
        end
      end

      if Shard.connection.supports_ddl_transactions? && self.shareable? && other_shard
        Shard.transaction do
          other_shard.activate do
            ::ActiveRecord::Base.connection.transaction(&create_shard)
          end
        end
      else
        create_shard.call
      end
    end

    def cache_store
      unless @cache_store
        @cache_store = Switchman.config[:cache_map][self.id] || Switchman.config[:cache_map][::Rails.env]
      end
      @cache_store
    end

    def shard_name(shard)
      if config[:shard_name]
        config[:shard_name]
      elsif config[:adapter] == 'postgresql'
        if shard == :bootstrap
          # rescue nil because the database may not exist yet; if it doesn't,
          # it will shortly, and this will be re-invoked
          ::ActiveRecord::Base.connection.schemas.first rescue nil
        else
          shard.activate { ::ActiveRecord::Base.connection_pool.default_schema }
        end
      else
        config[:database]
      end
    end

    private
    def reset_column_information
      ::ActiveRecord::Base.connection.schema_cache.clear!
      ::ActiveRecord::Base.descendants.reject{ |m| m == Shard }.each(&:reset_column_information)
    end
  end
end
